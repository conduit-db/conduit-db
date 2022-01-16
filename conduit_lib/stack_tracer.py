"""Stack tracer for multi-threaded applications.


Usage:

import stacktracer
stacktracer.start_trace("trace.html",interval=5,auto=True) # Set auto flag to always update file!
....
stacktracer.stop_trace()
"""

import sys
import traceback
from pygments import highlight
from pygments.formatters.html import HtmlFormatter
from pygments.lexers.python import PythonLexer


# Taken from http://bzimmer.ziclix.com/2008/12/17/python-thread-dumps/
from typing import cast


def stacktraces() -> str:
    code = []
    for threadId, stack in sys._current_frames().items():
        code.append("\n# ThreadID: %s" % threadId)
        for filename, lineno, name, line in traceback.extract_stack(stack):
            code.append('File: "%s", line %d, in %s' % (filename, lineno, name))
            if line:
                code.append("  %s" % (line.strip()))

    return cast(str, highlight("\n".join(code), PythonLexer(),
        HtmlFormatter(full=False,  # style="native",
        noclasses=True, )))


# This part was made by nagylzs
import os
import time
import threading


class TraceDumper(threading.Thread):
    """Dump stack traces into a given file periodically."""

    def __init__(self, fpath: str, interval: float, auto: bool) -> None:
        """
        @param fpath: File path to output HTML (stack trace file)
        @param auto: Set flag (True) to update trace continuously.
            Clear flag (False) to update only if file not exists.
            (Then delete the file to force update.)
        @param interval: In seconds: how often to update the trace file.
        """
        assert interval > 0.1
        self.auto = auto
        self.interval = interval
        self.fpath = os.path.abspath(fpath)
        self.stop_requested = threading.Event()
        threading.Thread.__init__(self, daemon=True)

    def run(self) -> None:
        while not self.stop_requested.is_set():
            time.sleep(self.interval)
            if self.auto or not os.path.isfile(self.fpath):
                self.stacktraces()

    def stop(self) -> None:
        self.stop_requested.set()
        self.join()
        try:
            if os.path.isfile(self.fpath):
                os.unlink(self.fpath)
        except:
            pass

    def stacktraces(self) -> None:
        fout = open(self.fpath, "w+")
        try:
            output = stacktraces()
            fout.write(output)
        finally:
            fout.close()


_tracer = None


def trace_start(fpath: str, interval: float=5, auto: bool=True) -> None:
    """Start tracing into the given file."""
    global _tracer
    if _tracer is None:
        _tracer = TraceDumper(fpath, interval, auto)
        _tracer.start()
    else:
        raise Exception("Already tracing to %s" % _tracer.fpath)


def trace_stop() -> None:
    """Stop tracing."""
    global _tracer
    if _tracer is None:
        raise Exception("Not tracing, cannot stop.")
    else:
        _tracer.stop()
        _tracer = None
