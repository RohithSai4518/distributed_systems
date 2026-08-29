"""
Structured, Thread-Safe Logging Subsystem with ANSI colorization,
contextual node prefixes, and configurable log sinks.
"""

from enum import IntEnum
import os
import sys
import threading
import time
from typing import Optional


class LogLevel(IntEnum):
    DEBUG = 10
    INFO = 20
    WARN = 30
    ERROR = 40
    FATAL = 50


class Logger:
    """Thread-safe, high-performance distributed systems logger."""

    _COLORS = {
        LogLevel.DEBUG: "\033[36m",    # Cyan
        LogLevel.INFO: "\033[32m",     # Green
        LogLevel.WARN: "\033[33m",     # Yellow
        LogLevel.ERROR: "\033[31m",    # Red
        LogLevel.FATAL: "\033[35;1m",  # Bold Magenta
    }
    _RESET = "\033[0m"
    _MUTEX = threading.Lock()
    _GLOBAL_LEVEL = LogLevel.INFO

    def __init__(self, node_id: str = "GLOBAL", min_level: Optional[LogLevel] = None):
        self.node_id = node_id
        self.min_level = min_level or self._GLOBAL_LEVEL

    @classmethod
    def set_global_level(cls, level: LogLevel):
        cls._GLOBAL_LEVEL = level

    def _log(self, level: LogLevel, msg: str, *args, **kwargs):
        if level < self.min_level:
            return

        formatted_msg = msg % args if args else msg
        if kwargs:
            extra_str = " ".join(f"{k}={v}" for k, v in kwargs.items())
            formatted_msg = f"{formatted_msg} [{extra_str}]"

        timestamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        ms = int((time.time() % 1) * 1000)
        thread_name = threading.current_thread().name

        color = self._COLORS.get(level, "")
        level_name = level.name.ljust(5)

        line = (
            f"{color}[{timestamp}.{ms:03d}] [{level_name}] [{self.node_id}] "
            f"[{thread_name}] {formatted_msg}{self._RESET}\n"
        )

        with self._MUTEX:
            sys.stdout.write(line)
            sys.stdout.flush()

    def debug(self, msg: str, *args, **kwargs):
        self._log(LogLevel.DEBUG, msg, *args, **kwargs)

    def info(self, msg: str, *args, **kwargs):
        self._log(LogLevel.INFO, msg, *args, **kwargs)

    def warn(self, msg: str, *args, **kwargs):
        self._log(LogLevel.WARN, msg, *args, **kwargs)

    def error(self, msg: str, *args, **kwargs):
        self._log(LogLevel.ERROR, msg, *args, **kwargs)

    def fatal(self, msg: str, *args, **kwargs):
        self._log(LogLevel.FATAL, msg, *args, **kwargs)
