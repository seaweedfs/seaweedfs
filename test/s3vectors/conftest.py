import base64
import contextlib
import gzip
import hashlib
import hmac
import http.client
import json
import os
from pathlib import Path
import socket
import ssl
import subprocess
import time
import urllib.error
import urllib.request
import uuid
import zlib

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


def pytest_configure(config):
    config.addinivalue_line("markers", "worker: needs the admin server and the Lance maintenance worker")


def pytest_collection_modifyitems(config, items):
    # Deselected rather than skipped: CI rejects a leg that reports skips.
    if os.environ.get("VECTOR_WORKER"):
        return
    skipped = [item for item in items if item.get_closest_marker("worker")]
    if skipped:
        config.hook.pytest_deselected(items=skipped)
        items[:] = [item for item in items if item not in skipped]


@pytest.fixture(scope="module", params=combinations(), ids=lambda p: "-".join(p))
def cluster(request, tmp_path_factory):
    yield from run_cluster(request.param, tmp_path_factory, identities=True)


@pytest.fixture(scope="module", params=combinations(), ids=lambda p: "-".join(p))
def open_cluster(request, tmp_path_factory):
    """A cluster whose gateway has no S3 identities."""
    yield from run_cluster(request.param, tmp_path_factory, identities=False)


def write_tls(directory):
    """A CA and one certificate for localhost. Volume servers serve HTTPS and
    every gRPC listener (master, volume, filer, gateway, engine) serves TLS,
    all requiring client certificates, and every client presents the same one."""
    def openssl(*args):
        subprocess.run(["openssl", *args], check=True, capture_output=True, cwd=directory)
    openssl("req", "-x509", "-newkey", "rsa:2048", "-nodes", "-keyout", "ca.key", "-out", "ca.pem",
            "-subj", "/CN=s3vectors test CA", "-days", "2",
            "-addext", "basicConstraints=critical,CA:TRUE", "-addext", "keyUsage=critical,keyCertSign,cRLSign")
    openssl("req", "-newkey", "rsa:2048", "-nodes", "-keyout", "key.pem", "-out", "req.csr", "-subj", "/CN=localhost")
    (directory / "ext.cnf").write_text(
        "subjectAltName=DNS:localhost,IP:127.0.0.1\nextendedKeyUsage=serverAuth,clientAuth\nbasicConstraints=CA:FALSE\n")
    openssl("x509", "-req", "-in", "req.csr", "-CA", "ca.pem", "-CAkey", "ca.key", "-CAcreateserial",
            "-out", "cert.pem", "-days", "2", "-extfile", "ext.cnf")
    # A second identity the same CA signed, which the engine's allowlist
    # must keep out.
    openssl("req", "-newkey", "rsa:2048", "-nodes", "-keyout", "outsider.key", "-out", "outsider.csr", "-subj", "/CN=outsider")
    openssl("x509", "-req", "-in", "outsider.csr", "-CA", "ca.pem", "-CAkey", "ca.key", "-CAcreateserial",
            "-out", "outsider.pem", "-days", "2", "-extfile", "ext.cnf")
    cert, key, ca = (f'{key} = {json.dumps(str(directory / name), ensure_ascii=False)}\n' for key, name in (("cert", "cert.pem"), ("key", "key.pem"), ("ca", "ca.pem")))
    pem = cert + key + ca
    grpc = "".join(f"[grpc.{component}]\n{cert}{key}\n" for component in ("master", "volume", "filer", "s3", "admin", "worker"))
    # Clients (the gateway, the engine) accept only servers named localhost;
    # the engine serves only clients named localhost.
    grpc += f"[grpc.client]\n{cert}{key}allowed_server_commonNames = \"localhost\"\n"
    grpc += f"[grpc.vector]\n{cert}{key}allowed_commonNames = \"localhost\"\n"
    (directory / "security.toml").write_text(f"[https.volume]\n{pem}\n[https.client]\nenabled = true\n{pem}\n[grpc]\n{ca}\n{grpc}")


def probe_grpc_tls(name, port, directory, outsider=False):
    """A TLS gRPC listener must refuse plaintext HTTP/2 and a connection
    without the client certificate, and answer the HTTP/2 preface with it.
    With `outsider`, a certificate the CA signed for a name outside the
    listener's allowed_commonNames must be refused too."""
    preface = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"
    with socket.create_connection(("127.0.0.1", port), timeout=5) as sock:
        sock.sendall(preface)
        with contextlib.suppress(ConnectionError, TimeoutError):
            answer = sock.recv(16)
            # Nothing, or a TLS alert record; a plaintext server sends SETTINGS.
            assert not answer or answer[0] == 0x15, f"{name} gRPC answered plaintext HTTP/2: {answer!r}"
    context = ssl.create_default_context(cafile=directory / "ca.pem")
    context.set_alpn_protocols(["h2"])
    with contextlib.suppress(ConnectionError, TimeoutError, ssl.SSLError):
        with socket.create_connection(("127.0.0.1", port), timeout=5) as sock:
            with context.wrap_socket(sock, server_hostname="localhost") as tls:
                tls.sendall(preface)
                assert not tls.recv(16), f"{name} gRPC served a client without a certificate"
    if outsider:
        other = ssl.create_default_context(cafile=directory / "ca.pem")
        other.set_alpn_protocols(["h2"])
        other.load_cert_chain(directory / "outsider.pem", directory / "outsider.key")
        with contextlib.suppress(ConnectionError, TimeoutError, ssl.SSLError):
            with socket.create_connection(("127.0.0.1", port), timeout=5) as sock:
                with other.wrap_socket(sock, server_hostname="localhost") as tls:
                    tls.sendall(preface)
                    assert not tls.recv(16), f"{name} gRPC served a certificate outside allowed_commonNames"
    context.load_cert_chain(directory / "cert.pem", directory / "key.pem")
    with socket.create_connection(("127.0.0.1", port), timeout=5) as sock:
        with context.wrap_socket(sock, server_hostname="localhost") as tls:
            tls.sendall(preface)
            header = b""
            while len(header) < 9 and (chunk := tls.recv(9 - len(header))):
                header += chunk
            # The first server frame is SETTINGS (type 4).
            assert len(header) == 9 and header[3] == 4, \
                f"{name} gRPC did not answer the HTTP/2 preface over TLS: {header!r}"


def read_token(key, fid):
    def encode(data):
        return base64.urlsafe_b64encode(data).rstrip(b"=")
    signed = encode(b'{"alg":"HS256","typ":"JWT"}') + b"." + encode(json.dumps({"fid": fid}).encode())
    return (signed + b"." + encode(hmac.new(key.encode(), signed, hashlib.sha256).digest())).decode()


def probe_hardening(endpoint, filer, vol, directory, read_jwt_key):
    """The hardened switches must be enforced, not only configured: a TLS
    volume server refuses plain HTTP and a connection without the client
    certificate, a volume server under a read key refuses an unsigned
    read of a real chunk, and an encrypted chunk is stored as ciphertext."""
    scheme = "https" if os.environ.get("VECTOR_TLS") else "http"
    context = None
    if scheme == "https":
        context = ssl.create_default_context(cafile=directory / "ca.pem")
        for url, kwargs in ((f"http://127.0.0.1:{vol}/status", {}),
                            (f"https://127.0.0.1:{vol}/status", {"context": context})):
            try:
                with urllib.request.urlopen(url, timeout=5, **kwargs) as response:
                    assert response.status != 200, f"{url} answered without the client certificate"
            # Go answers plaintext with HTTP 400, Rust with a TLS alert.
            except (urllib.error.URLError, ConnectionError, ssl.SSLError, http.client.HTTPException):
                pass
        context.load_cert_chain(directory / "cert.pem", directory / "key.pem")
        with urllib.request.urlopen(f"https://127.0.0.1:{vol}/status", timeout=5, context=context) as response:
            assert response.status == 200
    encrypted = os.environ.get("VECTOR_ENCRYPT_VOLUMES")
    if not (read_jwt_key or encrypted):
        return
    s3 = boto3.client("s3", endpoint_url=endpoint, aws_access_key_id="test", aws_secret_access_key="test",
                      region_name="us-east-1", config=Config(s3={"addressing_style": "path"}))
    bucket = "hardening-" + uuid.uuid4().hex[:12]
    body = b"x" * 1024
    s3.create_bucket(Bucket=bucket)
    try:
        s3.put_object(Bucket=bucket, Key="chunk", Body=body)
        with urllib.request.urlopen(f"http://127.0.0.1:{filer}/buckets/{bucket}/chunk?metadata=true", timeout=5) as response:
            chunk = json.load(response)["chunks"][0]
        url = f"{scheme}://127.0.0.1:{vol}/{chunk['file_id']}"
        headers = {}
        if read_jwt_key:
            try:
                with urllib.request.urlopen(url, timeout=5, context=context):
                    raise AssertionError("the volume server served an unsigned read")
            except urllib.error.HTTPError as error:
                assert error.code == 401, error
            headers["Authorization"] = "Bearer " + read_token(read_jwt_key, chunk["file_id"])
        with urllib.request.urlopen(urllib.request.Request(url, headers=headers), timeout=5, context=context) as response:
            stored = response.read()
        if encrypted:
            assert chunk.get("cipher_key"), "the gateway stored a chunk without a cipher key"
            assert stored and stored != body, "the volume server holds the chunk in plaintext or not at all"
            with contextlib.suppress(OSError, EOFError, zlib.error):
                assert gzip.decompress(stored) != body, "the volume server holds the chunk gzipped, not encrypted"
        else:
            assert stored == body, "the signed read did not return the chunk"
    finally:
        s3.delete_object(Bucket=bucket, Key="chunk")
        s3.delete_bucket(Bucket=bucket)


def run_cluster(param, tmp_path_factory, identities):
    volume, mode = param
    directory = tmp_path_factory.mktemp(f"vectors-{volume}-{mode}" + ("" if identities else "-open"))
    print(f"\nVector integration logs: {directory}")
    weed = binary("WEED_BINARY", "weed")
    engine = binary("VECTOR_BINARY", "weed-vector")
    # VECTOR_WORKER=1 adds the admin server and the Lance maintenance worker,
    # for the tests that build vector indexes through a plugin job.
    lance_worker = binary("LANCE_WORKER_BINARY", "weed-worker") if os.environ.get("VECTOR_WORKER") else None
    worker = binary("VECTOR_WORKER_BINARY", "weed-vector-worker") if mode == "distributed" else None
    volume_binary = binary("RUST_VOLUME_SERVER_BINARY", "weed-volume") if volume == "rust" else weed
    processes = {}
    with contextlib.ExitStack() as stack:
        with contextlib.ExitStack() as reservations:
            ports = []
            for _ in range(15):
                sock = reservations.enter_context(socket.socket())
                sock.bind(("127.0.0.1", 0))
                ports.append(sock.getsockname()[1])
        (master, master_grpc, vol, vol_grpc, filer, filer_grpc, s3, s3_grpc, lance, vector, worker1, worker2,
         admin, admin_grpc, vector_metrics) = ports
        master_addr = f"127.0.0.1:{master}.{master_grpc}"
        filer_addr = f"127.0.0.1:{filer}.{filer_grpc}"

        # VECTOR_READ_JWT=1 makes volume reads need a JWT; every weed process
        # and the workers sign with the key.
        read_jwt_key = os.environ.get("VECTOR_READ_JWT") and uuid.uuid4().hex
        # The cluster's filer signing key: the gateway's catalog authenticates
        # the worker and the CLI by the token they derive from it.
        filer_signing_key = uuid.uuid4().hex
        # VECTOR_TLS=1 puts the volume servers behind HTTPS with client
        # certificates; every process finds security.toml in its cwd.
        if os.environ.get("VECTOR_TLS"):
            write_tls(directory)

        def process_env(unset=()):
            env = dict(os.environ, AWS_ACCESS_KEY_ID="test", AWS_SECRET_ACCESS_KEY="test",
                       AWS_DEFAULT_REGION="us-east-1", AWS_EC2_METADATA_DISABLED="true",
                       WEED_S3_SSE_KEY="s3vectors-test", WEED_JWT_FILER_SIGNING_KEY=filer_signing_key,
                       **({"WEED_JWT_SIGNING_READ_KEY": read_jwt_key} if read_jwt_key else {}))
            env.pop("WEED_S3_SSE_KEK", None)
            if not read_jwt_key:
                env.pop("WEED_JWT_SIGNING_READ_KEY", None)
            for key in unset:
                env.pop(key)
            return env

        def start(name, args, unset=()):
            log = stack.enter_context((directory / f"{name}.log").open("w"))
            process = subprocess.Popen(args, cwd=directory, stdout=log, stderr=log, env=process_env(unset))
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
            "max": 64, "preStopSeconds": 0,
        }.items()])
        wait_for(lambda: listening(vol_grpc), processes, "volume not ready")
        # VECTOR_ENCRYPT_VOLUMES=1 turns on the filer's volume encryption, which
        # the gateway, the engine and the worker all follow: every chunk they
        # store is encrypted, and direct reads must undo it.
        filer_group = os.environ.get("VECTOR_FILER_GROUP", "")
        encrypt = ["-encryptVolumeData"] if os.environ.get("VECTOR_ENCRYPT_VOLUMES") else []
        start("filer", [weed, "filer", f"-master={master_addr}", "-ip=127.0.0.1",
                        "-ip.bind=127.0.0.1", f"-port={filer}", f"-port.grpc={filer_grpc}",
                        f"-defaultStoreDir={directory / 'filer'}", f"-filerGroup={filer_group}", *encrypt])
        wait_for(lambda: listening(filer_grpc), processes, "filer not ready")
        endpoint = f"http://127.0.0.1:{s3}"
        # weed s3 turns AWS_ACCESS_KEY_ID into an admin identity.
        if identities:
            config, unset = [f"-config={HERE / 's3.json'}"], ()
        else:
            config, unset = [], ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY")
        start("s3", [weed, "-v=1", "s3", f"-filer={filer_addr}", *config,
                     "-ip=127.0.0.1", "-ip.bind=127.0.0.1", f"-port={s3}",
                     f"-port.grpc={s3_grpc}", "-port.iceberg=0", f"-port.lance={lance}"], unset)
        wait_for(lambda: http_ready(f"http://127.0.0.1:{lance}/v1/namespace/root/list"), processes, "catalog not ready")
        wait_for(lambda: listening(s3), processes, "s3 not ready")
        if os.environ.get("VECTOR_TLS") or read_jwt_key or encrypt:
            probe_hardening(endpoint, filer, vol, directory, read_jwt_key)
        # The engine keeps tables on the filers and volume servers itself; it
        # has no S3 credentials to be given, and encrypts its chunks for the
        # volume servers as the gateway does.
        start("vector", [engine, "--masters", master_addr, "--listen", f"127.0.0.1:{vector}", "--metrics-port", str(vector_metrics), "--filer-group", filer_group])
        wait_for(lambda: listening(vector), processes, "vector engine not ready")
        def vector_ready():
            with urllib.request.urlopen(f"http://127.0.0.1:{vector_metrics}/ready", timeout=1) as response:
                return response.status == 200
        wait_for(vector_ready, processes, "vector engine did not register with master")
        if os.environ.get("VECTOR_TLS"):
            wait_for(lambda: listening(s3_grpc), processes, "s3 gRPC not ready")
            for name, port in (("master", master_grpc), ("volume", vol_grpc), ("filer", filer_grpc),
                               ("s3", s3_grpc)):
                probe_grpc_tls(name, port, directory)
            probe_grpc_tls("engine", vector, directory, outsider=True)

        wait_for(
            lambda: f"Cluster: vector node 127.0.0.1:{vector} added to group '{filer_group}'"
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
        admin_url = None
        if lance_worker:
            (directory / "admin").mkdir()
            admin_url = f"http://127.0.0.1:{admin}"
            start("admin", [weed, "admin", f"-filerGroup={filer_group}", "-ip=127.0.0.1", f"-port={admin}", f"-port.grpc={admin_grpc}",
                            "-port.legacy=0", f"-master={master_addr}", f"-dataDir={directory / 'admin'}"])
            wait_for(lambda: http_ready(f"{admin_url}/api/plugin/job-types"), processes, "admin not ready")
            tls_args = []
            if os.environ.get("VECTOR_TLS"):
                tls_args = ["--tls-ca", str(directory / "ca.pem"), "--tls-cert", str(directory / "cert.pem"), "--tls-key", str(directory / "key.pem"), "--tls-server-name", "localhost"]
            start("lance-worker", [lance_worker, "--admin", f"127.0.0.1:{admin}.{admin_grpc}",
                                   "--namespace", f"http://127.0.0.1:{lance}", "--id", "vectors-test-worker",
                                   "--masters", master_addr, "--filer-group", filer_group, *tls_args])
            wait_for(lambda: "lance_vector_index" in
                     [jt["job_type"] for jt in json.load(urllib.request.urlopen(f"{admin_url}/api/plugin/job-types", timeout=2))],
                     processes, "the Lance worker never registered with admin")

        yield {"endpoint": endpoint, "mode": mode, "processes": processes, "directory": directory, "filer": filer,
               "masters": master_addr, "filer_signing_key": filer_signing_key,
               "vector_metrics": f"http://127.0.0.1:{vector_metrics}",
               "catalog": f"http://127.0.0.1:{lance}", "admin": admin_url, "env": process_env()}


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
