import threading

class IntentXSLock:
    def __init__(self):
        self._lock = threading.Lock()
        self.ix_count = 0
        self.is_count = 0
        self.s_count = 0
        self.x_count = 0

    """
    # Called when pickled (warning, this does not change any counters)
    # https://stackoverflow.com/questions/50441786/pickle-cant-pickle-thread-lock-objects
    """
    def __getstate__(self):
        state = self.__dict__.copy()
        del state['_lock']
        return state
    
    """
    # Called when unpickled (warning, this does not change any counters)
    """
    def __setstate__(self, state):
        self.__dict__.update(state)
        self._lock = threading.Lock()
    
    def acquire_S(self):
        with self._lock:
            if self.ix_count > 0 or self.x_count > 0:
                raise Exception("Cannot acquire shared lock")
            self.s_count += 1
    
    def acquire_X(self):
        with self._lock:
            if self.is_count > 0 or self.ix_count > 0 or self.s_count > 0 or self.x_count > 0:
                raise Exception("Cannot acquire exclusive lock")
            self.x_count += 1
    
    def acquire_IS(self):
        with self._lock:
            if self.x_count > 0:
                raise Exception("Cannot acquire intent-to-share lock")
            self.is_count += 1
    
    def acquire_IX(self):
        with self._lock:
            if self.s_count > 0 or self.x_count > 0:
                raise Exception("Cannot acquire intent-to-exclusivity lock")
            self.ix_count += 1
    
    def release_S(self):
        with self._lock:
            if self.s_count < 1:
                raise Exception("Shared lock released too many times")
            self.s_count -= 1
    
    def release_X(self):
        with self._lock:
            if self.x_count < 1:
                raise Exception("Exclusive lock released too many times")
            self.x_count -= 1
    
    def release_IS(self):
        with self._lock:
            if self.is_count < 1:
                raise Exception("Intent-to-share lock released too many times")
            self.is_count -= 1
    
    def release_IX(self):
        with self._lock:
            if self.ix_count < 1:
                raise Exception("Intent-to-exclusivity lock released too many times")
            self.ix_count -= 1
