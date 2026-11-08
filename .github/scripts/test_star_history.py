import importlib.util
import os
from pathlib import Path
import sys
import types
import unittest
from unittest.mock import patch
from urllib.error import HTTPError


def load_star_history():
    matplotlib = types.ModuleType("matplotlib")
    matplotlib.use = lambda backend: None
    pyplot = types.ModuleType("matplotlib.pyplot")
    dates = types.ModuleType("matplotlib.dates")
    dates.AutoDateLocator = object
    dates.DateFormatter = object
    modules = {
        "matplotlib": matplotlib,
        "matplotlib.pyplot": pyplot,
        "matplotlib.dates": dates,
    }
    with patch.dict(sys.modules, modules), patch.dict(os.environ, {"GITHUB_TOKEN": "test"}):
        path = Path(__file__).with_name("star_history.py")
        spec = importlib.util.spec_from_file_location("star_history", path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
    return module


star_history = load_star_history()


class StarHistoryTests(unittest.TestCase):
    def http_error(self, code):
        return HTTPError("https://api.github.com", code, "error", {}, None)

    @patch.object(star_history.time, "sleep")
    @patch.object(star_history.urllib.request, "urlopen")
    def test_retries_server_error(self, urlopen, sleep):
        response = object()
        urlopen.side_effect = [self.http_error(500), response]

        result = star_history.open_url(object())

        self.assertIs(result, response)
        self.assertEqual(urlopen.call_count, 2)
        sleep.assert_called_once_with(1)

    @patch.object(star_history.time, "sleep")
    @patch.object(star_history.urllib.request, "urlopen")
    def test_does_not_retry_non_transient_http_error(self, urlopen, sleep):
        error = self.http_error(404)
        urlopen.side_effect = error

        with self.assertRaises(HTTPError):
            star_history.open_url(object())

        urlopen.assert_called_once()
        sleep.assert_not_called()

    @patch.object(star_history.time, "sleep")
    @patch.object(star_history.urllib.request, "urlopen")
    def test_stops_after_retry_limit(self, urlopen, sleep):
        urlopen.side_effect = self.http_error(500)

        with self.assertRaises(HTTPError):
            star_history.open_url(object())

        self.assertEqual(urlopen.call_count, star_history.REQUEST_ATTEMPTS)
        self.assertEqual(sleep.call_count, star_history.REQUEST_ATTEMPTS - 1)


if __name__ == "__main__":
    unittest.main()
