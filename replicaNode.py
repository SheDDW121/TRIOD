import pika
import json
import sys

from config import num_storages, print_each_step, durability, chunk_size, print_every_chunk

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
        try:

            request = json.loads(body)
            command = request['command']
            if command == 'COPY':
                row = request['data']
                date = row['date_parsed']
                if date not in self.data:
                    self.data[date] = []
                self.data[date].append(row)

                if print_each_step:
                    print(f"[Реплика-{self.storage_id}] Копия данных сохранена: {row}")

            elif command == 'COPY_2':

                received_data = request['data']
                reply_to = request['reply_to']
                replica_id = request['replica_id']
                chunk_id = request['chunk_id']
                total_chunks = request['total_chunks']
                
                self.data.update(received_data)

                if print_every_chunk:
                    print(f"[Реплика-{self.storage_id}] Восстановленные данные чанка {chunk_id + 1} сохранены")


            elif command == 'RELOCATE':

                def chunk_data(data, chunk_size=1000):
                    """Функция для разбиения словаря на части."""
                    items = list(data.items())
                    for i in range(0, len(items), chunk_size):
                        chunk = dict(items[i:i + chunk_size])
                        yield chunk

                relocation_storage_id = request['storage_id']

                # Считаем количество чанков для разбиения данных
                total_chunks = (len(self.data) + (chunk_size - 1)) // chunk_size  # Примерно, делим на chunk_size и округляем вверх


                # Разбиваем данные на части
                chunks = chunk_data(self.data, chunk_size=chunk_size)  # Можно выбрать размер чанка
                
                # Для каждого чанка отправляем отдельное сообщение
                for id, chunk in enumerate(chunks):
                    load_request = {
                        'command': 'LOAD_2',
                        'data': chunk,  # Отправляем только одну часть данных
                        'reply_to': request['reply_to'],
                        'replica_id': self.storage_id,
                        'chunk_id': id,  # Индекс чанка для восстановления
                        'total_chunks': total_chunks  # Общее количество чанков
                    }
                    
                    # Отправляем сообщение в RabbitMQ
                    self.channel.basic_publish(
                        exchange='',
                        routing_key=f'storage-{relocation_storage_id}',
                        body=json.dumps(load_request)
                    )

                print(f"[Реплика-{self.storage_id}] Отправила все собственные данные хранителю с id {relocation_storage_id} (разбила данные на {total_chunks} чанков для пересылки)")

                print(f"[Реплика-{self.storage_id}] Завершаю работу.")
                self.channel.stop_consuming() 
                self.connection.close()

                raise SystemExit() 
                # return
            
            elif command == 'GET':
                date = request['date']

                if date in self.data:
                    response = self.data[date]

                    # Добавляем дополнительные сведения
                    response_with_info = {
                        'data': response,
                        'node_id': self.storage_id,
                        'queue_name': "TEST"
                    }

                    self.channel.basic_publish(exchange='', routing_key=request['reply_to'], body=json.dumps(response_with_info))
                    print(f"[Реплика-{self.storage_id}] Отправлены данные за {date}")
                else:
                    response = "Данных за указанную дату не найдено"

                    # Добавляем дополнительные сведения
                    response_with_info = {
                        'data': response,
                        'node_id': self.storage_id,
                        'queue_name': "SSS"
                    }

                    print(f"[Реплика-{self.storage_id}] Данные за {date} не найдены")
                    self.channel.basic_publish(exchange='', routing_key=request['reply_to'], body=json.dumps(response_with_info))

        except Exception as e:
            print(f"[Реплика {self.storage_id}]: Ошибка {e}")
            
    
    def start(self):
        print(f"[Реплика-{self.storage_id}] Запущена и ждет запросы...")
        self.channel.start_consuming()
