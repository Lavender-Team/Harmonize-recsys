import threading

# 특정 key에 대한 lock을 관리하는 클래스
class KeyLockDict:
    def __init__(self):
        self.data = {}
        self.locks = {}
        self.global_lock = threading.Lock()

    def _get_lock(self, key):
        # 특정 key에 대한 lock을 생성 또는 반환
        with self.global_lock:
            if key not in self.locks:
                self.locks[key] = threading.Lock()
            return self.locks[key]

    def set(self, key, value):
        lock = self._get_lock(key)
        with lock:
            self.data[key] = value

    def get(self, key):
        lock = self._get_lock(key)
        with lock:
            return self.data.get(key)

    def add(self, key, value):
        lock = self._get_lock(key)
        with lock:
            if self.data.get(key) is not None:
                self.data[key] += value
            else:
                self.data[key] = 3 + value

    def remove(self, key):
        lock = self._get_lock(key)
        with lock:
            if key in self.data:
                del self.data[key]
                del self.locks[key]