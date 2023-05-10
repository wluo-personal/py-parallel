from multiprocessing import Manager
import ctypes
from contextlib import contextmanager
import time
class CrossProcessLock:
    _idle_ = 1e-8

    @classmethod
    @contextmanager
    def lock(cls, value: Manager().Value):
        """
        cannot totally get rid of the race condition
        value should be: Manager().Value(ctypes.c_bool, False)
        :return:
        """
        while value.value:
            time.sleep(cls._idle_)
        value.set(True)
        try:
            yield True
        finally:
            value.set(False)

