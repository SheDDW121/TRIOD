import hashlib
import bisect

from config import hash_prefix

class ConsistentHashing:
    def __init__(self, num_storages):
        self.num_storages = num_storages
        self.ring = {}
        self.sorted_keys = []
        
        # Добавляем хранителей
        for i in range(num_storages):
            self.add_storage(i)

    def _hash(self, key):
        """Возвращает хэш-значение для ключа (даты или хранителя)"""
        return int(hashlib.md5(key.encode()).hexdigest(), 16)
    
    def add_storage(self, storage_id):
        """Добавляет хранитель на кольцо с виртуальными узлами"""
        storage_key = f"{hash_prefix}{storage_id}"
        hashed_key = self._hash(storage_key)
        self.ring[hashed_key] = storage_id
        bisect.insort(self.sorted_keys, hashed_key)

    def remove_storage(self, storage_id):
        """Удаляет хранитель с кольца"""
        # Вычисляем хеш для данного хранителя

        storage_key = f"{hash_prefix}{storage_id}"
        hashed_key = self._hash(storage_key)

        # Удаляем его из кольца и списка
        if hashed_key in self.ring:
            del self.ring[hashed_key]
            self.sorted_keys.remove(hashed_key)

    def get_storage(self, key):
        """Определяет, какому хранителю принадлежит ключ (дата)"""
        hashed_key = self._hash(key)

        # Поиск ближайшего хэша по кольцу
        index = bisect.bisect(self.sorted_keys, hashed_key)
        if index == len(self.sorted_keys):  
            index = 0  # Если достигли конца списка, идем на начало

        storage_id = self.ring[self.sorted_keys[index]]
        return storage_id
