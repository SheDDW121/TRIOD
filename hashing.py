import hashlib
import bisect

class ConsistentHashing:
    def __init__(self, num_storages, virtual_nodes=3):
        self.num_storages = num_storages
        self.virtual_nodes = virtual_nodes  # Количество виртуальных узлов на хранитель
        self.ring = {}
        self.sorted_keys = []
        self.total_vnodes = num_storages * virtual_nodes  # Общее количество виртуальных узлов
        
        # Добавляем хранителей
        for i in range(num_storages):
            self.add_storage(i)

    def _hash(self, key):
        """Возвращает хэш-значение для ключа (даты или хранителя)"""
        return int(hashlib.md5(key.encode()).hexdigest(), 16)

    # def _get_virtual_hash(self, vnode_index):
    #     """Равномерно распределяет хэши виртуальных узлов по кольцу"""
    #     max_hash = 2**128  # Максимальное значение md5
    #     return (vnode_index + 1) * (max_hash // self.total_vnodes) 
    
    def add_storage(self, storage_id):
        """Добавляет хранитель на кольцо с виртуальными узлами"""
        for i in range(self.virtual_nodes):
            virtual_key = f"storage-{storage_id}-vnode-{i}"
            hashed_key = self._hash(virtual_key)
            self.ring[hashed_key] = storage_id
            bisect.insort(self.sorted_keys, hashed_key)

            # vnode_index = storage_id * self.virtual_nodes + i  # Уникальный индекс для виртуального узла
            # hashed_key = self._get_virtual_hash(vnode_index)   # Равномерный хеш, но этот способ пока не используем
            # self.ring[hashed_key] = storage_id                 # Так как тогда при добавлении новых будет неравномерно и все пересчитывать не хочется
            # bisect.insort(self.sorted_keys, hashed_key)

    def get_storage(self, key):
        """Определяет, какому хранителю принадлежит ключ (дата)"""
        hashed_key = self._hash(key)

        # Поиск ближайшего хэша по кольцу
        index = bisect.bisect(self.sorted_keys, hashed_key)
        if index == len(self.sorted_keys):  
            index = 0  # Если достигли конца списка, идем на начало

        storage_id = self.ring[self.sorted_keys[index]]
        return storage_id
