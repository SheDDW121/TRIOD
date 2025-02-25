from datetime import datetime
import time
import numpy as np
import pandas as pd
import pika
import json
import sys
from hashing import ConsistentHashing

from config import num_storages, num_vnodes, print_each_step, durability

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
        self.consistent_hashing = ConsistentHashing(num_storages, num_vnodes)

        # Подключение к RabbitMQ
        self.connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        self.channel = self.connection.channel()

        self.channel.queue_declare(queue='manager_responses', durable=durability)
        self.channel.queue_declare(queue='manager_commands', durable=durability)
        self.channel.queue_declare(queue='client_responses', durable=durability)

        # Создаем очереди для хранителей
        for i in range(num_storages):
            queue_name = f"storage-{i}"
            self.channel.queue_declare(queue=queue_name, durable=durability)

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
        print(f"[Manager] Запрос GET {date} -> storage_{storage_node}")

    # def listen_responses(self, timeout=2):
        # """Ожидает ответов от хранителей в течение заданного времени (секунды)."""
        # print(f"[Manager] Ожидание ответов от хранителей в течение {timeout} секунд...")

        # responses = []

        # def callback(ch, method, properties, body):
        #     response = json.loads(body)
        #     print(f"[Manager] Получен ответ: {response}")
        #     responses.append(response)

        #     # Отправляем ответ клиенту
        #     self.channel.basic_publish(
        #         exchange='',
        #         routing_key='client_responses',
        #         body=json.dumps(response)
        #     )

        #     # self.channel.basic_cancel('manager_responses')

        # # Подписываемся на очередь ответов
        # self.channel.basic_consume(queue='manager_responses', on_message_callback=callback, auto_ack=True)

        # # Ждем ответы в течение timeout секунд
        # start_time = time.time()
        # while time.time() - start_time < timeout:
        #     self.connection.process_data_events(time_limit=0.5)  # Проверяем очередь сообщений

        # # Отписываемся от очереди после тайм-аута
        # self.channel.basic_cancel('manager_responses')

        # print("[Manager] Завершено ожидание ответов.")
        # return responses


    # def listen_responses(self): # блокирующий consume
    #     """Ожидает ответов от хранителей"""
    #     def callback(ch, method, properties, body):
    #         response = json.loads(body)
    #         print(f"[Manager] Получен ответ: {response}")
        
    #     self.channel.basic_consume(queue='manager_responses', on_message_callback=callback, auto_ack=True)
    #     print("[Manager] Ожидание ответов от хранителей...")
    #     self.channel.start_consuming()

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

                if print_each_step:
                    print(f"[Менеджер] Отправил данные {row} в {queue_name}")

            print("[Менеджер] Данные успешно загружены и распределены!")
            return {"status": "OK", "message": f"Файл {file_path} загружен"}
        
        except Exception as e:

            print(f"[Ошибка] Не удалось загрузить файл: {e}")
            return {"status": "ERROR", "message": f"Не удалось загрузить файл: {e}"}
        
            # self.channel = self.connection.channel()
    
    def on_client_command(self, ch, method, properties, body):
        message = json.loads(body)
        print(message)
        command = message.get("command").strip().split()
        cmd = command[0].upper()

        if cmd == "LOAD":

            if len(command) < 2:
                print("[Ошибка] Использование: LOAD [имя файла]")
                response = {"status": "ERROR", "message": "[Ошибка] Использование: LOAD [имя файла]"}
                return

            file_name = command[1]
            response = self.load_data(file_name)

        elif cmd == "GET":

            if len(command) < 2:
                print("[Ошибка] Использование: GET [дата]")
                response = {"status": "ERROR", "message": "[Ошибка] Использование: GET [дата]"}
                return
                
            date = command[1]
            self.send_get_request(date)
            # self.listen_responses()

            response = {"status": "OK", "message": f"Запрос GET {date} отправлен"}

        else:
            response = {"status": "ERROR", "message": "Неизвестная команда"}

        # Отправляем ответ клиенту
        self.channel.basic_publish(
            exchange='',
            routing_key='client_responses',
            body=json.dumps(response)
        )

    def on_storage_message(self, ch, method, properties, body):
        response = json.loads(body)
        print(f"[Manager] Получен ответ: {response}")
        # responses.append(response)
        
        # Отправляем ответ клиенту
        self.channel.basic_publish(
            exchange='',
            routing_key='client_responses',
            body=json.dumps(response)
        )

        # self.channel.basic_cancel('manager_responses')

        # Подписываемся на очередь ответов
    def start(self):
        print("[Менеджер] Ожидание команд от клиента...")
        self.channel.basic_consume(queue='manager_commands', on_message_callback=self.on_client_command, auto_ack=True)
        self.channel.basic_consume(queue='manager_responses', on_message_callback=self.on_storage_message, auto_ack=True)
        self.channel.start_consuming()

    # def start(self):
    #     """
    #     Ожидает команд от пользователя (LOAD, GET).
    #     """
    #     print("[Менеджер] Ожидание команд...")
    #     while True:
    #         command = input("> ").strip().split()

    #         if not command:
    #             continue
            
    #         cmd = command[0].upper()

    #         if cmd == "LOAD":
    #             if len(command) < 2:
    #                 print("[Ошибка] Использование: LOAD [имя файла]")
    #                 continue
    #             file_name = command[1]
    #             self.load_data(file_name)

    #         elif cmd == "GET":
    #             if len(command) < 2:
    #                 print("[Ошибка] Использование: GET [дата]")
    #                 continue
    #             date = command[1]
    #             self.send_get_request(date)
    #             self.listen_responses()

    #         elif cmd == "EXIT":
    #             print("[Менеджер] Завершение работы.")
    #             break

    #         else:
    #             print("[Ошибка] Неизвестная команда. Доступные команды: LOAD [файл], GET [дата], EXIT.")

    #     self.connection.close()

    

if __name__ == "__main__":
    manager = StorageManager(num_storages=num_storages, num_vnodes=num_vnodes)
    manager.start()
