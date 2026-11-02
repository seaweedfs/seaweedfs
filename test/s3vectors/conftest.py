import contextlib
import os
from pathlib import Path
import socket
import subprocess
import time
import urllib.error
import urllib.request

import boto3
from botocore.config import Config
import pytest

HERE = Path(__file__).resolve().parent


def combinations():
    volumes = os.environ.get("VOLUME_SERVER_IMPL", "go,rust").split(",")
    default_modes = "single,distributed" if (HERE / "../../rust/sw-vector-distributed/Cargo.toml").is_file() else "single"
    modes = os.environ.get("VECTOR_QUERY_MODE", default_modes).split(",")
    if not set(volumes) <= {"go", "rust"} or not set(modes) <= {"single", "distributed"}:
        raise ValueError("invalid VOLUME_SERVER_IMPL or VECTOR_QUERY_MODE")
    return [(volume, mode) for volume in volumes for mode in modes]


def binary(variable, name):
    path = Path(os.environ.get(variable, str(HERE / name))).resolve()
    if not path.is_file() or not os.access(path, os.X_OK):
        pytest.fail(f"Missing executable {path}; run make build or set {variable}")
    return str(path)


def wait_for(check, processes, message):
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        for name, process in processes.items():
            if process.poll() is not None:
                pytest.fail(f"{name} exited with {process.returncode}; {message}")
        try:
            if check():
                return
        except (OSError, urllib.error.URLError):
            pass
        time.sleep(0.2)
    pytest.fail(message)


def listening(port):
    with socket.create_connection(("127.0.0.1", port), timeout=0.5):
        return True


def http_ready(url):
    try:
        with urllib.request.urlopen(url, timeout=0.5):
            return True
    except urllib.error.HTTPError:
        return True


def stop(process):
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=10)


@pytest.fixture(scope="module", params=combinations(), ids=lambda p: "-".join(p))
def cluster(request, tmp_path_factory):
    volume, mode = request.param
    directory = tmp_path_factory.mktemp(f"vectors-{volume}-{mode}")
    print(f"\nVector integration logs: {directory}")
    weed = binary("WEED_BINARY", "weed")
    engine = binary("VECTOR_BINARY", "weed-vector")
    worker = binary("VECTOR_WORKER_BINARY", "weed-vector-worker") if mode == "distributed" else None
    volume_binary = binary("RUST_VOLUME_SERVER_BINARY", "weed-volume") if volume == "rust" else weed
    processes = {}
    with contextlib.ExitStack() as stack:
        with contextlib.ExitStack() as reservations:
            ports = []
            for _ in range(12):
                sock = reservations.enter_context(socket.socket())
                sock.bind(("127.0.0.1", 0))
                ports.append(sock.getsockname()[1])
        master, master_grpc, vol, vol_grpc, filer, filer_grpc, s3, s3_grpc, lance, vector, worker1, worker2 = ports
        master_addr = f"127.0.0.1:{master}.{master_grpc}"
        filer_addr = f"127.0.0.1:{filer}.{filer_grpc}"

        def start(name, args):
            log = stack.enter_context((directory / f"{name}.log").open("w"))
            env = dict(os.environ, AWS_ACCESS_KEY_ID="test", AWS_SECRET_ACCESS_KEY="test",
                       AWS_DEFAULT_REGION="us-east-1", AWS_EC2_METADATA_DISABLED="true",
                       WEED_S3_SSE_KEY="s3vectors-test")
            process = subprocess.Popen(args, cwd=directory, stdout=log, stderr=log, env=env)
            processes[name] = process
            stack.callback(stop, process)
            return process

        for name in ("master", "volume", "filer"):
            (directory / name).mkdir()
        start("master", [weed, "-v=1", "master", "-ip=127.0.0.1", "-ip.bind=127.0.0.1",
                         f"-port={master}", f"-port.grpc={master_grpc}",
                         f"-mdir={directory / 'master'}", "-peers=none", "-volumeSizeLimitMB=32"])
        wait_for(lambda: http_ready(f"http://127.0.0.1:{master}/dir/status"), processes, "master not ready")
        prefix = [volume_binary] if volume == "rust" else [weed, "volume"]
        flag = "--" if volume == "rust" else "-"
        start("volume", prefix + [f"{flag}{key}={value}" for key, value in {
            "master": master_addr, "ip": "127.0.0.1", "ip.bind": "127.0.0.1",
            "port": vol, "port.grpc": vol_grpc, "dir": directory / "volume",
            "max": 16, "preStopSeconds": 0,
        }.items()])
        wait_for(lambda: listening(vol_grpc), processes, "volume not ready")
        start("filer", [weed, "filer", f"-master={master_addr}", "-ip=127.0.0.1",
                        "-ip.bind=127.0.0.1", f"-port={filer}", f"-port.grpc={filer_grpc}",
                        f"-defaultStoreDir={directory / 'filer'}"])
        wait_for(lambda: listening(filer_grpc), processes, "filer not ready")
        endpoint = f"http://127.0.0.1:{s3}"
        start("s3", [weed, "-v=1", "s3", f"-filer={filer_addr}", f"-config={HERE / 's3.json'}",
                     "-ip=127.0.0.1", "-ip.bind=127.0.0.1", f"-port={s3}",
                     f"-port.grpc={s3_grpc}", "-port.iceberg=0", f"-port.lance={lance}"])
        wait_for(lambda: http_ready(f"http://127.0.0.1:{lance}/v1/namespace/root/list"), processes, "catalog not ready")
        start("vector", [engine, "--namespace", f"http://127.0.0.1:{lance}",
                         "--masters", master_addr, "--listen", f"127.0.0.1:{vector}",
                         "--storage-option", "aws_access_key_id=test",
                         "--storage-option", "aws_secret_access_key=test"])
        wait_for(lambda: listening(vector), processes, "vector engine not ready")

        wait_for(
            lambda: f"Cluster: vector node 127.0.0.1:{vector} added to group ''"
            in (directory / "master.log").read_text(),
            processes,
            "vector engine never registered with the master",
        )

        if worker:
            for i, port in enumerate((worker1, worker2)):
                start(f"worker-{i}", [worker, "--masters", master_addr, "--listen", f"127.0.0.1:{port}"])
                wait_for(lambda: listening(port), processes, "vector worker not ready")
            wait_for(
                lambda: all(
                    f"S3 Vectors: discovered search worker 127.0.0.1:{port}"
                    in (directory / "s3.log").read_text()
                    for port in (worker1, worker2)
                ),
                processes,
                "gateway did not discover both search workers",
            )
        yield {"endpoint": endpoint, "mode": mode, "processes": processes, "directory": directory}


@pytest.fixture(scope="module")
def vectors(cluster):
    return boto3.client("s3vectors", endpoint_url=cluster["endpoint"],
                        aws_access_key_id="test", aws_secret_access_key="test", region_name="us-east-1",
                        config=Config(connect_timeout=3, read_timeout=20, retries={"max_attempts": 0}))


@pytest.fixture(scope="module")
def vector_clients(vectors):
    return {
        name: boto3.client("s3vectors", endpoint_url=vectors.meta.endpoint_url,
                           aws_access_key_id=name, aws_secret_access_key=name + "-secret",
                           region_name="us-east-1", config=vectors.meta.config)
        for name in ("reader", "writer")
    }
