import io
import unittest
from unittest.mock import patch

from sharesight_console import warn


class TtyBuffer(io.StringIO):
    def isatty(self):
        return True


class ConsoleTests(unittest.TestCase):
    def test_warning_is_yellow_on_a_colour_capable_terminal(self):
        stream = TtyBuffer()
        with patch.dict("os.environ", {}, clear=True):
            warn("Something differs", stream=stream)
        self.assertEqual(
            stream.getvalue(),
            "\033[33mWARNING: Something differs\033[0m\n",
        )


if __name__ == "__main__":
    unittest.main()
