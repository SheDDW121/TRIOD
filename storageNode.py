import os
import time
import pika
import json
import pandas as pd
from multiprocessing import Process
from replicaNode import ReplicaNode

from config import num_storages, print_each_step, durability, print_only_if_dead, print_every_chunk

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
        self.channel.queue_declare(queue='manager_responses', durable=durability) # Очередь для ответов хранителей и реплик (просматриваем менеджером)
        self.channel.queue_declare(queue='manager_pings', durable=durability) # Очередь для ответов хранителей на команду PING (просматриваем менеджером)

        self.channel.queue_declare(queue='showcase_data', durable=durability) # Очередь для передачи данных на процесс-витрину

        self.channel.basic_consume(queue=self.queue_name, on_message_callback=self.handle_request, auto_ack=True)

        print(f"[Хранитель-{self.node_id}] Запущен и ожидает запросов...")

    def handle_request(self, ch, method, properties, body):
        try:

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

                load_request = {'command': 'LOAD', 'data': row}

                # Отправляем данные в реплику
                self.channel.basic_publish(
                    exchange='', routing_key=self.replica_queue, 
                    body=json.dumps(load_request)
                )


                # Отправляем данные также в очередь витрины
                self.channel.basic_publish(
                    exchange='',
                    routing_key='showcase_data',
                    body=json.dumps(load_request)
                )

                if print_each_step:
                    print(f"[Хранитель-{self.node_id}] Отправил копию данных в {self.replica_queue}")

                # if date in self.data:
                #     del self.data[date]  # Удаляем данные по дате для тестирования ответа реплики

            elif command == 'LOAD_2': # Получена команда загрузки данных из реплики в случае падения одного из хранителей
                received_data = request['data']
                reply_to = request['reply_to']
                replica_id = request['replica_id']
                chunk_id = request['chunk_id']
                total_chunks = request['total_chunks']

                last_chunk = chunk_id == total_chunks - 1


                self.data.update(received_data)
                
                if chunk_id + 1 != total_chunks: # если не последний чанк
                    if print_every_chunk:
                        print(f"[Хранитель-{self.node_id}] Получил и восстановил чанк данных {chunk_id + 1}/{total_chunks} от реплики {replica_id}")
                else:
                    print(f"[Хранитель-{self.node_id}] Получил и восстановил все чанки данных {chunk_id + 1}/{total_chunks} от реплики {replica_id}")

                response = (f"[Хранитель-{self.node_id}] Получил и восстановил чанк данных {chunk_id + 1}/{total_chunks} от реплики {replica_id}")

                # Добавляем дополнительные сведения
                response_with_info = {
                    'data': response,
                    'node_id': self.node_id,
                    'queue_name': self.queue_name
                }

                # пересылаем ли ответ менеджеру, что успешно восстановили каждый чанк, или только последний (а значит, и все)
                if print_every_chunk or last_chunk:
                    self.channel.basic_publish(exchange='', routing_key=reply_to, body=json.dumps(response_with_info))

                copy_2_request = {
                    'command': 'COPY_2',
                    'data': received_data,  # Отправляем только одну часть данных
                    'reply_to': request['reply_to'],
                    'replica_id': replica_id,
                    'chunk_id': chunk_id,  # Индекс чанка для восстановления
                    'total_chunks': total_chunks  # Общее количество чанков
                }

                # Отправляем запрос реплике на копирование восстановленных данных
                self.channel.basic_publish(exchange='', routing_key=self.replica_queue, body=json.dumps(copy_2_request))

                if print_every_chunk:
                    print(f"[Хранитель-{self.node_id}] Отправил восстановленную копию данных от реплики {replica_id} в {self.replica_queue}")
            
            elif command == 'PING':

                response = "Запрос PING был получен хранителем"

                reply_to = request['reply_to']

                # Добавляем дополнительные сведения
                response_with_info = {
                    'data': response,
                    'node_id': self.node_id,
                    'queue_name': self.queue_name,
                    'answer': "PONG"
                }
                if not print_only_if_dead:
                    print(f"[Хранитель-{self.node_id}] Получен PING от менеджера")

                # Отправляем ответ менеджеру
                self.channel.basic_publish(exchange='', routing_key=reply_to, body=json.dumps(response_with_info))

            elif command == "KILL":
                print(f"[Хранитель-{self.node_id}] Получен запрос на остановку. Завершаю работу (имитация, что отказал).")
                self.channel.stop_consuming() 
                self.connection.close()
                raise SystemExit() 
                # return


            elif command == 'GET':
                date = request['date']
                reply_to = request['reply_to']

                if date in self.data:
                    response = self.data[date]

                    # Добавляем дополнительные сведения
                    response_with_info = {
                        'data': response,
                        'node_id': self.node_id,
                        'queue_name': self.queue_name
                    }

                    # Отправляем ответ менеджеру
                    self.channel.basic_publish(exchange='', routing_key=reply_to, body=json.dumps(response_with_info))
                    print(f"[Хранитель {self.node_id}] Найдены данные за {date}, отправил менеджеру")
                else:
                    print(f"[Хранитель-{self.node_id}] Данные за {date} не найдены")

                    response = "Данных за указанную дату не найдено"

                    # Добавляем дополнительные сведения
                    response_with_info = {
                        'data': response,
                        'node_id': self.node_id,
                        'queue_name': self.queue_name
                    }

                    # Отправляем ответ менеджеру
                    self.channel.basic_publish(exchange='', routing_key=reply_to, body=json.dumps(response_with_info))

                    # self.channel.basic_publish(
                    #     exchange='', routing_key=self.replica_queue, body=json.dumps({'command': 'GET', 'date': date, 'reply_to': reply_to})
                    # )

            else: 
                print(f"[Хранитель {self.node_id}] {request} Получил неизвестный запрос от менеджера")

        except Exception as e:
            print(f"[Хранитель {self.node_id}], Ошибка: {e}")


    def start(self):
        """
        Запускает процесс ожидания сообщений.
        """
        self.channel.start_consuming()

# def run_storage(node_id):
#     """Функция для запуска хранителя и реплики"""
#     # Создаем и запускаем реплику в отдельном процессе

#     replica_process = Process(target=run_replica, args=(node_id,))
#     replica_process.start()

#     # Запускаем хранитель
#     storage = StorageNode(node_id)
#     storage.start()

# def run_replica(node_id):
#     """Функция для запуска реплики"""
#     replica = ReplicaNode(node_id)
#     replica.start()

def run_storage(node_id):
    """Функция для запуска хранителя и реплики"""
    replica_process = Process(target=run_replica, args=(node_id,))
    replica_process.start()

    storage_process = Process(target=start_storage, args=(node_id,), name=f"StorageNode_{node_id}")
    storage_process.start()

    return storage_process 

def start_storage(node_id):
    """Запуск хранителя внутри процесса"""
    storage = StorageNode(node_id)
    storage.start()


def run_replica(node_id):
    """Функция для запуска реплики"""
    replica = ReplicaNode(node_id)
    replica.start()

if __name__ == "__main__":

    processes = {}

    for i in range(num_storages):  
        p = run_storage(i) 
        processes[i] = p 

    time.sleep(1.5)
    print("Запущены процессы:")
    counter = 0
    for k, v in processes.items():
        print(f"node_id = {k} PID = {v.pid}", end=", " if counter < 2 else "\n")
        counter = (counter + 1) % 3

    for p in processes.values():
        p.join()

