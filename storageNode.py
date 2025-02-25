import time
import pika
import json
import pandas as pd
from multiprocessing import Process
from replicaNode import ReplicaNode

from config import num_storages, num_vnodes, print_each_step, durability

class StorageNode:
    def __init__(self, node_id):
        self.node_id = node_id
        self.data = {} # Здесь будем хранить строки (ключ - дата, значение - список записей)
        self.replica_queue = f'replica-{node_id}'

        # Подключение к RabbitMQ
        self.connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        self.channel = self.connection.channel()

        # Объявляем очередь
        self.queue_name = f"storage-{node_id}"
        self.channel.queue_declare(queue=self.queue_name, durable=durability)
        self.channel.queue_declare(queue=self.replica_queue, durable=durability)

        self.channel.basic_consume(queue=self.queue_name, on_message_callback=self.handle_request, auto_ack=True)

        print(f"[Хранитель-{self.node_id}] Запущен и ожидает запросов...")

    def handle_request(self, ch, method, properties, body):
        """Обрабатывает запросы (LOAD, GET)"""

        request = json.loads(body)
        command = request.get('command')

        if command == 'LOAD':
            row = request['data']
            date = row['date_parsed']
            if date not in self.data:
                self.data[date] = []
            self.data[date].append(row) # Добавляем данные в словарь

            if print_each_step:
                print(f"[Хранитель-{self.node_id}] Получил и сохранил данные за {date}: {row}")

            # Отправляем данные в реплику
            self.channel.basic_publish(
                exchange='', routing_key=self.replica_queue, 
                body=json.dumps({'command': 'COPY', 'data': row})
            )

            if print_each_step:
                print(f"[Хранитель-{self.node_id}] Отправил копию данных в {self.replica_queue}")

            # if date in self.data:
            #     del self.data[date]  # Удаляем данные по дате для тестирования ответа реплики

        elif command == 'GET':
            date = request['date']
            reply_to = request['reply_to']

            if date in self.data:
                response = self.data[date]

                # Отправляем ответ менеджеру
                self.channel.basic_publish(exchange='', routing_key=reply_to, body=json.dumps(response))
                print(f"[Хранитель {self.node_id}] Найдены данные за {date}, отправил менеджеру")
            else:
                print(f"[Хранитель-{self.node_id}] Данные за {date} не найдены, запрашиваю у реплики... Затем реплика пришлет ответ менеджеру")

                self.channel.basic_publish(
                    exchange='', routing_key=self.replica_queue, body=json.dumps({'command': 'GET', 'date': date, 'reply_to': reply_to})
                )

        else: 
            print(f"[Хранитель {self.node_id}] {request} Получил неизвестный запрос от менеджера")

    def start(self):
        """
        Запускает процесс ожидания сообщений.
        """
        self.channel.start_consuming()

def run_storage(node_id):
    """Функция для запуска хранителя и реплики"""
    # Создаем и запускаем реплику в отдельном процессе

    replica_process = Process(target=run_replica, args=(node_id,))
    replica_process.start()

    # Запускаем хранитель
    storage = StorageNode(node_id)
    storage.start()

def run_replica(node_id):
    """Функция для запуска реплики"""
    replica = ReplicaNode(node_id)
    replica.start()

if __name__ == "__main__":
    processes = []
    for i in range(num_storages):  
        # Запускаем num_storages хранителей
        p = Process(target=run_storage, args=(i,))
        p.start()
        processes.append(p)

        # replica_process = Process(target=run_replica, args=(i,))
        # replica_process.start()
        # processes.append(replica_process)

    for p in processes:
        p.join()
