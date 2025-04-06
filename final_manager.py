from datetime import datetime
import threading
import time
import numpy as np
import pandas as pd
import pika
import json
import sys
from hashing import ConsistentHashing

from config import num_storages, print_each_step, durability, ping_interval, max_retries, print_only_if_dead, hash_prefix

def convert_date(date_str):

    # Третий формат: 20000120 (годмесяцдень)
    try:
        date_str = str(int(date_str))  # Преобразуем в строку, отбрасывая десятичную часть
        return datetime.strptime(date_str, "%Y%m%d").strftime("%d-%m-%Y")
    except ValueError:
        pass

    # Первый формат: 2012-01-31 (год-месяц-число)
    try:
        return datetime.strptime(date_str, "%Y-%m-%d").strftime("%d-%m-%Y")
    except ValueError:
        pass

    # Второй формат: 19970527-15:00 (годмесяцдень-время)
    try:
        return datetime.strptime(date_str.split('-')[0], "%Y%m%d").strftime("%d-%m-%Y")
    except ValueError:
        pass

    # Если не удается распознать формат
    return None

class StorageManager:
    def __init__(self, num_storages, num_vnodes=3):
        self.num_storages = num_storages
        self.num_vnodes = num_vnodes
        self.consistent_hashing = ConsistentHashing(num_storages)


        self.pending_pings = {}
        self.failed_pings = {}  # Storages, которые не ответили на пинг

        self.ping_interval = ping_interval
        # self.retry_interval = 1  # Интервал повторных пингов
        self.max_retries = max_retries  # Максимальное количество попыток повторного пинга
        self.lock = threading.Lock()

        # Подключение к RabbitMQ
        self.connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        self.channel = self.connection.channel()

        self.channel.queue_declare(queue='manager_responses', durable=durability) # Очередь для ответов хранителей и реплик (просматриваем менеджером)
        self.channel.queue_declare(queue='manager_commands', durable=durability) # Очередь для запросов клиентов (просматриваем менеджером)
        self.channel.queue_declare(queue='client_responses', durable=durability) # Очередь для публикации ответов менеджера клиенту (отправляем менеджером, просматриваем клиентом)

        self.channel.queue_declare(queue='manager_pings', durable=durability) # Очередь для ответов хранителей на команду PING (просматриваем менеджером)

        self.channel.queue_declare(queue='showcase_data', durable=durability) # Очередь для передачи данных на процесс-витрину

        # Создаем очереди для хранителей
        for i in range(num_storages):
            queue_name = f"storage-{i}"
            self.channel.queue_declare(queue=queue_name, durable=durability) # Очередь для отправки хранителям запросов (отправляем менеджером, просматриваем хранителями)

        self.live_storages = set(range(num_storages))  # Живые хранители
        self.dead_storages = set()  # Упавшие хранители
        
        # Запуск фонового потока пинга
        self.ping_thread = threading.Thread(target=self.ping_storages, daemon=True)
        self.ping_thread.start()

        # Запуск прослушивания очереди ответов на пинги
        self.ping_listener_thread = threading.Thread(target=self.listen_ping_responses, daemon=True)
        self.ping_listener_thread.start()

    def ping_storages(self):
        """Периодически проверяет, живы ли хранители (в отдельном потоке)"""
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
            channel = connection.channel()

            with self.lock:
                self.failed_pings = {storage: 0 for storage in range(self.num_storages)}

            while True:
                with self.lock:
                    self.pending_pings = {storage: time.time() for storage in self.live_storages.union(self.dead_storages)}
                for storage_id in range(self.num_storages):
                    if storage_id in self.dead_storages:
                        continue  # Не пингуем, если хранитель уже упал

                    request = {'command': 'PING', 'reply_to': 'manager_pings'}
                    channel.basic_publish(exchange='', routing_key=f'storage-{storage_id}', body=json.dumps(request))

                time.sleep(self.ping_interval)  # Периодичность пинга

                self.check_pending_pings(channel)

        except Exception as e:
            print(f"[Ошибка] в ping_storages: {e}")

    def check_pending_pings(self, channel):
        """Проверка, какие хранители не ответили на PING"""
        for storage_id in list(self.pending_pings.keys()):
            if self.failed_pings[storage_id] < self.max_retries: # очередной раз не было ответа на PING
                with self.lock:
                    self.failed_pings[storage_id] += 1
                    print(f"[Менеджер] Хранитель {storage_id} не отправил ответ на PING ({self.failed_pings[storage_id]}/{self.max_retries}) раз")

                if self.failed_pings[storage_id] >= self.max_retries: # логика кода если хранитель "умер"
                    self.mark_storage_dead(storage_id)
                    print(f"Хранитель {storage_id} был отмечен как 'dead' после {self.max_retries} неудачных попыток")

                    # удаляем из круга и ключей в consistent hashing
                    self.consistent_hashing.remove_storage(storage_id)

                    # определярем в consistent hashing id следующего хранителя по хешу умершего хранителя
                    relocation_storage_id = self.consistent_hashing.get_storage(f'{hash_prefix}{storage_id}')

                    # отправляем команду на релоцирование данных реплике умершего хранителя
                    request = {'command': 'RELOCATE', 'reply_to': 'manager_responses', 'storage_id': relocation_storage_id}
                    channel.basic_publish(exchange='', routing_key=f'replica-{storage_id}', body=json.dumps(request))

                    print(f"[Менеджер] Отправил запрос на релоцирование реплике {storage_id}")

            else:
                # print("NOT SUPPOSED TO BE HERE")
                # self.mark_storage_dead(storage_id)
                if not print_only_if_dead:
                    print(f"Хранитель {storage_id} был отмечен как 'dead' после {self.max_retries} неудачных попыток")

    def mark_storage_dead(self, storage_id):
        """Помечает хранителя как мертвого"""
        with self.lock:
            self.dead_storages.add(storage_id)
            self.live_storages.discard(storage_id)
            del self.pending_pings[storage_id]
            print(f"Хранитель {storage_id} был отмечен как 'dead'")

    def listen_ping_responses(self):
        """Этот метод будет слушать очередь с ответами на пинг (manager_pings)."""
        try:

            def callback(ch, method, properties, body):
                response = json.loads(body)
                if not print_only_if_dead:
                    print(f"[Менеджер] Получен ответ PONG от хранителя: {response}")

                storage_id = response.get('node_id')
                if storage_id in self.pending_pings: # находится в словаре тех, кто должен получить пинг, но еще не был обработан
                    with self.lock:
                        self.live_storages.add(storage_id)
                        self.dead_storages.discard(storage_id)
                        del self.pending_pings[storage_id]
                        self.failed_pings[storage_id] = 0 # обнуляем неудачные попытки, так как PING был получен
                    if not print_only_if_dead:
                        print(f"Хранитель {storage_id} находится в рабочем состоянии")
            
            connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
            channel = connection.channel()
            channel.basic_consume(queue='manager_pings', on_message_callback=callback, auto_ack=True)
            if not print_only_if_dead:
                print("[Менеджер] Ожидаю ответы на пинги...")
            channel.start_consuming()

        except Exception as e:
            print(f"[Ошибка] в listen_ping_responses: {e}")

    def get_storage(self, date):
        """Определяет, какой хранитель должен хранить дату"""
        return self.consistent_hashing.get_storage(date)

    def send_get_request(self, date):
        """Отправляет запрос на получение данных"""
        storage_node = self.get_storage(date)
        request = {'command': 'GET', 'date': date, 'reply_to': 'manager_responses'}
        self.channel.basic_publish(
            exchange='', routing_key=f'storage-{storage_node}', body=json.dumps(request)
        )
        print(f"[Менеджер] Запрос GET {date} -> storage_{storage_node}")

        return {"status": "OK", "message": f"Запрос GET {date} отправлен"}

    def load_data(self, file_path):
        """
        Загружает CSV, разбивает данные по хранителям и отправляет их в RabbitMQ.
        """
        try:
            data = pd.read_csv(file_path)

            # Возможные имена столбцов с датой
            possible_date_columns = ['date', 'datetime_utc', 'Date.Full', 'DATE']

            # Проверяем, какой из столбцов присутствует в DataFrame
            date_column = None
            for column in possible_date_columns:
                if column in data.columns:
                    date_column = column
                    break

            for _, row in data.iterrows():
                date_raw = row[date_column]
                date_parsed = convert_date(date_raw)
                row['date_parsed'] = date_parsed
                storage_id = self.consistent_hashing.get_storage(date_parsed)
                queue_name = f"storage-{storage_id}"
                
                load_request = {'command': 'LOAD', 'data': row.to_dict()}
                self.channel.basic_publish(exchange='', routing_key=queue_name, body=json.dumps(load_request))

                # # отправляем в очередь витрины
                # self.channel.basic_publish(
                #     exchange='',
                #     routing_key='showcase_data',
                #     body=json.dumps(load_request)
                # )

                if print_each_step:
                    print(f"[Менеджер] Отправил данные {row} в {queue_name}")

            print("[Менеджер] Данные успешно загружены и распределены!")
            return {"status": "OK", "message": f"Файл {file_path} загружен"}
        
        except Exception as e:

            print(f"[Ошибка] Не удалось загрузить файл: {e}")
            return {"status": "ERROR", "message": f"Не удалось загрузить файл: {e}"}
        
            # self.channel = self.connection.channel()
    
    def on_client_command(self, ch, method, properties, body):
        try:

            message = json.loads(body)
            print(message)
            command = message.get("command").strip().split()
            cmd = command[0].upper()

            if cmd == "LOAD":

                if len(command) < 2:
                    print("[Ошибка] Использование: LOAD [имя файла]")
                    response = {"status": "ERROR", "message": "[Ошибка] Использование: LOAD [имя файла]"}

                else:

                    file_name = command[1]
                    response = self.load_data(file_name)

            elif cmd == "GET":

                if len(command) < 2:
                    print("[Ошибка] Использование: GET [дата]")
                    response = {"status": "ERROR", "message": "[Ошибка] Использование: GET [дата]"}

                else:
                    date = command[1]
                    response = self.send_get_request(date)

            elif cmd == "KILL":
                """Отправляет запрос на получение данных"""

                if len(command) < 2:
                    print("[Ошибка] Использование: KILL [ID хранителя]")
                    response = {"status": "ERROR", "message": "[Ошибка] Использование: KILL [ID хранителя]"}
                
                else:
                    request = {'command': 'KILL', 'reply_to': 'manager_responses'}

                    storage_node = int(command[1])
                    self.channel.basic_publish(
                        exchange='', routing_key=f'storage-{storage_node}', body=json.dumps(request)
                    )
                    print(f"[Менеджер] Запрос KILL -> storage_{storage_node}")

                    response = {"status": "OK", "message": f"Запрос KILL {storage_node} отправлен"}

            else:
                response = {"status": "ERROR", "message": "Неизвестная команда"}

            # Отправляем ответ клиенту
            self.channel.basic_publish(
                exchange='',
                routing_key='client_responses',
                body=json.dumps(response)
            )
        except Exception as e:

            print(f"[Ошибка] {e}")

    def on_storage_message(self, ch, method, properties, body):
        response = json.loads(body)

        print(f"[Менеджер] Получен ответ от хранителя с id = {response['node_id']} (queue_name = {response['queue_name']}): {response['data']}")
        
        # Отправляем ответ клиенту
        self.channel.basic_publish(
            exchange='',
            routing_key='client_responses',
            body=json.dumps(response)
        )

    def start(self):
        print("[Менеджер] Ожидание команд от клиента...")
        self.channel.basic_consume(queue='manager_commands', on_message_callback=self.on_client_command, auto_ack=True)
        self.channel.basic_consume(queue='manager_responses', on_message_callback=self.on_storage_message, auto_ack=True)
        self.channel.start_consuming()
    

if __name__ == "__main__":
    manager = StorageManager(num_storages=num_storages)
    manager.start()
