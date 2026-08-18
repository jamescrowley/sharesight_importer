import os
import sys


YELLOW = "\033[33m"
RED = "\033[31m"
RESET = "\033[0m"


def warn(message, stream=None):
    _write("WARNING", message, YELLOW, stream)


def error(message, stream=None):
    _write("ERROR", message, RED, stream)


def _write(label, message, colour, stream):
    stream = stream or sys.stderr
    text = f"{label}: {message}"
    if stream.isatty() and "NO_COLOR" not in os.environ:
        text = f"{colour}{text}{RESET}"
    print(text, file=stream)
