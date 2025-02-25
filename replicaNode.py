import pika
import json
import sys

from config import num_storages, num_vnodes, print_each_step, durability

class ReplicaNode:
    def __init__(self, storage_id):
        self.storage_id = storage_id
        self.data = {}  # Хранилище данных (ключ - дата, значение - список записей)

        # Подключение к RabbitMQ
        self.connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        self.channel = self.connection.channel()

        # Объявляем очередь для реплики
        self.replica_queue = f'replica-{storage_id}'
        self.channel.queue_declare(queue=self.replica_queue, durable=durability)

        # Устанавливаем обработчик сообщений
        self.channel.basic_consume(
            queue=self.replica_queue, on_message_callback=self.handle_request, auto_ack=True
        )

        print(f'[Реплика-{self.storage_id}] Запущена и ожидает данные...')
        self.channel.start_consuming()

    def handle_request(self, ch, method, properties, body):
        request = json.loads(body)
        if request['command'] == 'COPY':
            row = request['data']
            date = row['date_parsed']
            if date not in self.data:
                self.data[date] = []
            self.data[date].append(row)

            if print_each_step:
                print(f"[Реплика-{self.storage_id}] Копия данных сохранена: {row}")
        
        elif request['command'] == 'GET':
            date = request['date']

            if date in self.data:
                response = self.data[date]
                self.channel.basic_publish(exchange='', routing_key=request['reply_to'], body=json.dumps(response))
                print(f"[Реплика-{self.storage_id}] Отправлены данные за {date}")
            else:
                response = "Данных за указанную дату не найдено"
                print(f"[Реплика-{self.storage_id}] Данные за {date} не найдены")
                self.channel.basic_publish(exchange='', routing_key=request['reply_to'], body=json.dumps(response))
            
    
    def start(self):
        print(f"[Реплика-{self.storage_id}] Запущена и ждет запросы...")
        self.channel.start_consuming()
