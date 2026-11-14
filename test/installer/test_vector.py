import io
import os
from pathlib import Path
import subprocess
import tarfile
import tempfile
import unittest
import zipfile

SOURCE = (Path(__file__).resolve().parents[2] / 'install.sh').read_text().rsplit('\nmain\n', 1)[0]
BASH = os.environ.get('BASH_BINARY', 'bash')


class VectorInstallerTest(unittest.TestCase):
    def shell(self, code, **env):
        result = subprocess.run([BASH, '-s'], input=(SOURCE + '\n' + code + '\n').encode('utf-8'),
                                env=dict(os.environ, **env), capture_output=True)
        result.stdout = result.stdout.decode('utf-8', errors='replace')
        result.stderr = result.stderr.decode('utf-8', errors='replace')
        return result

    def test_platforms_and_asset_names(self):
        for system, arch, supported, suffix in [
            ('linux', 'amd64', True, 'tar.gz'), ('linux', 'arm64', True, 'tar.gz'),
            ('darwin', 'amd64', True, 'tar.gz'), ('darwin', 'arm64', True, 'tar.gz'),
            ('windows', 'amd64', True, 'zip'), ('windows', 'arm64', False, 'zip'),
            ('freebsd', 'amd64', False, 'tar.gz'),
        ]:
            with self.subTest(system=system, arch=arch):
                result = self.shell('vector_platform_supported "$SYSTEM" "$ARCH"; result=$?; '
                                    'vector_asset_name "$SYSTEM" "$ARCH"; exit "$result"', SYSTEM=system, ARCH=arch)
                if supported:
                    self.assertEqual(result.returncode, 0, result.stderr)
                    self.assertEqual(result.stdout.strip(), f'weed-vector_{system}_{arch}.{suffix}')
                else:
                    self.assertNotEqual(result.returncode, 0)

    def test_installs_archive_binary(self):
        for system in ['linux', 'darwin', 'windows']:
            with self.subTest(system=system), tempfile.TemporaryDirectory() as directory:
                directory = Path(directory)
                name = 'weed-vector.exe' if system == 'windows' else 'weed-vector'
                contents = b'vector engine fixture'
                archive = directory / ('vector.zip' if system == 'windows' else 'vector.tar.gz')
                if system == 'windows':
                    with zipfile.ZipFile(archive, 'w') as output:
                        output.writestr(name, contents)
                else:
                    with tarfile.open(archive, 'w:gz') as output:
                        info = tarfile.TarInfo(name)
                        info.size = len(contents)
                        info.mode = 0o755
                        output.addfile(info, io.BytesIO(contents))
                dest = directory / 'bin'
                result = self.shell('download() { cp "$ARCHIVE" "$2"; }; VERSION=test; '
                                    'INSTALL_DIR="$DEST"; install_component vector "$SYSTEM" amd64',
                                    ARCHIVE=archive.as_posix(), DEST=dest.as_posix(), SYSTEM=system)
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
                self.assertEqual((dest / name).read_bytes(), contents)


class EnterpriseInstallerTest(unittest.TestCase):
    shell = VectorInstallerTest.shell

    def enterprise(self, code, **env):
        return self.shell('ENTERPRISE=true; REPO=seaweedfs/artifactory; EDITION=-enterprise; ' + code, **env)

    def test_asset_names(self):
        result = self.enterprise('go_asset_name linux amd64; go_asset_name windows amd64; '
                                 'rust_asset_name linux arm64; worker_asset_name linux amd64; '
                                 'vector_asset_name linux arm64; LARGE_DISK=true; '
                                 'go_asset_name darwin arm64; rust_asset_name linux amd64')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.split(), [
            'weed-enterprise-linux_amd64.tar.gz', 'weed-enterprise-windows_amd64.zip',
            'weed-volume-enterprise_linux_arm64.tar.gz', 'weed-worker-enterprise_linux_amd64.tar.gz',
            'weed-vector-enterprise_linux_arm64.tar.gz', 'weed-enterprise-darwin_arm64_large_disk.tar.gz',
            'weed-volume-enterprise_large_disk_linux_amd64.tar.gz',
        ])

    def test_open_source_go_asset_names(self):
        result = self.shell('go_asset_name linux amd64; go_asset_name windows amd64; '
                            'LARGE_DISK=true; go_asset_name windows amd64')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.split(), [
            'linux_amd64.tar.gz', 'windows_amd64.zip', 'windows_amd64_large_disk.zip',
        ])

    def test_windows_weed_keeps_exe(self):
        with tempfile.TemporaryDirectory() as directory:
            directory = Path(directory)
            archive = directory / 'weed.zip'
            with zipfile.ZipFile(archive, 'w') as output:
                output.writestr('weed.exe', b'weed fixture')
            dest = directory / 'bin'
            result = self.shell('download() { cp "$ARCHIVE" "$2"; }; VERSION=test; '
                                'INSTALL_DIR="$DEST"; install_component weed windows amd64',
                                ARCHIVE=archive.as_posix(), DEST=dest.as_posix())
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            self.assertEqual((dest / 'weed.exe').read_bytes(), b'weed fixture')

    def test_rust_binaries_linux_only(self):
        for system, arch, supported in [
            ('linux', 'amd64', True), ('linux', 'arm64', True),
            ('darwin', 'arm64', False), ('windows', 'amd64', False),
        ]:
            with self.subTest(system=system, arch=arch):
                for check in ['vector_platform_supported', 'rust_volume_platform_supported']:
                    result = self.enterprise(f'{check} "$SYSTEM" "$ARCH"', SYSTEM=system, ARCH=arch)
                    self.assertEqual(result.returncode == 0, supported, check)

    def test_latest_version_skips_other_releases(self):
        tags = ['vfs-0.2.5', 'kvcache-filer-native-v0.1.0-preview.1', '4.54-rc1', '4.53', '4.52']
        listing = ''.join(f'  "tag_name": "{tag}",\\n' for tag in tags)
        result = self.enterprise(f"curl() {{ printf '{listing}'; }}; get_latest_version")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), '4.53')
