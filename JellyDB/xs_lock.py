from __future__ import annotations
import threading

class XSLock:
    def __init__(self):
        self._lock = threading.Lock()
        self._share_count = 0
        self._exclusive_count = 0

    def __enter__(self):
        # Used with `with` statement. The lock will already have been acquired when this is called.
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        if exception_type is not None:
            print("An exception of type {} with value {} and traceback {} should just have been raised. If it wasn't, raise an exception in __exit__ of XSLock. If it was, remove the if clause that printed this message.".format(exception_type, exception_value, traceback))
        self.release()

    # Returns itself
    def acquire_X(self) -> XSLock:
        if self.acquire_X_bool() == False:
            raise Exception("Could not acquire exclusive lock")
        return self

    # Returns itself
    def acquire_S(self) -> XSLock:
        if self.acquire_S_bool() == False:
            raise Exception("Could not acquire shared lock")
        return self

    # Returns success of acquiring exclusive lock
    def acquire_X_bool(self) -> bool:
        with self._lock:
            if self._share_count > 0 or self._exclusive_count > 0:
                return False
            self._exclusive_count = 1
            return True

    # Returns success of acquiring shared lock
    def acquire_S_bool(self) -> bool:
        with self._lock:
            if self._exclusive_count > 0:
                return False
            self._share_count += 1
            return True

    # Releases one lock - inferring which kind to release.
    def release(self):
        with self._lock:
            if self._share_count > 0:
                self._share_count -= 1
            elif self._exclusive_count > 0:
                self._exclusive_count = 0
            else:
                raise Exception("There are no S or X locks to release")
