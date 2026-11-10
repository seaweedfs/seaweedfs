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
