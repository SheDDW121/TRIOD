import threading
import pika
import json
import time

from config import num_storages, print_each_step, durability

def send_command_to_manager(command):
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    
    channel.queue_declare(queue='manager_commands', durable=durability) # Очередь для запросов клиентов (просматриваем менеджером)
    channel.queue_declare(queue='client_responses', durable=durability) # Очередь для просмотров ответа менеджера и витрины (просматриваем клиентом)
    
    request = {'command': command, 'reply_to': 'client_responses'}
    channel.basic_publish(exchange='', routing_key='manager_commands', body=json.dumps(request))
    print(f"[Клиент] Отправлена команда: {command}")
    
    connection.close()

def send_command_to_showcase(command, date1, date2):
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    
    channel.queue_declare(queue='showcase_requests', durable=durability) # Очередь для передачи запросов на процесс-витрину (от клиента)
    channel.queue_declare(queue='client_responses', durable=durability) # Очередь для просмотров ответа менеджера и витрины (просматриваем клиентом)

    # Формируем JSON-объект с полями: command, date1, date2
    request = {
        'command': command,
        'date1': date1,
        'date2': date2,
        'reply_to': 'client_responses'  # Указываем очередь для ответа
    }

    # Отправляем сообщение в очередь 'showcase_requests'
    channel.basic_publish(
        exchange='',
        routing_key='showcase_requests',
        body=json.dumps(request)
    )
    print(f"[Клиент] Отправлена команда на витрину: {command} с датами: {date1} и {date2}")

    # Закрываем соединение
    connection.close()

def listen_responses():
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    
    channel.queue_declare(queue='client_responses', durable=durability)
    
    def callback(ch, method, properties, body):
        response = json.loads(body)

        if 'node_id' in response and 'queue_name' in response:
            print(f"[Клиент] Получен ответ от менеджера. Данные получены от хранителя с id = {response['node_id']} (queue_name = {response['queue_name']}): {response['data']}")

        elif 'from' in response:
            print(f"[Клиент] Получен ответ от витрины: ", end='')

            if (response['status'] == "404"):
                print('Не найдено данных за указанный период времени')

            elif (response['status'] == "204"):
                print("Витрина пустая и не содержит данных")
            
            elif (response['status'] == "500"):
                print(f"На витрине произошла ошибка при обработке запроса: {response['message']}")
                
            elif response['from'] == 'showcase1':
                # Проходим по всем ключам и значениям словаря
                print('')
                for key, value in response['data'].items():
                    print(f"{key}: {value}")

            # elif response['from'] == 'showcase2':
            else:
                print(f"{response['avg_temperature']}")

        else:
            print(f"[Клиент] Получен ответ от менеджера: {response}")

        print("\r> ", end="", flush=True)  # Курсор в начало строки
    
    channel.basic_consume(queue='client_responses', on_message_callback=callback, auto_ack=True)
    # print("[Клиент] Ожидание ответов...")
    
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        print("[Клиент] Завершение работы.")
        channel.stop_consuming()
        connection.close()

if __name__ == "__main__":

    # Запуск потока для получения ответов
    listener_thread = threading.Thread(target=listen_responses, daemon=True)
    listener_thread.start()
    
    print("[Клиент] Введите команды: LOAD [файл], GET [дата], KILL [nodeID], temp_range [date1 date2], temp_range_avg [date1 date2] EXIT")
    while True:
        command = input("> ").strip()
        if command.upper() == "EXIT":
            print("[Клиент] Завершение работы.")
            break
        elif command.startswith("temp_range"):
            parts = command.split(" ")
            if len(parts) == 3:
                start_date = parts[1]
                end_date = parts[2]
                send_command_to_showcase(parts[0], parts[1], parts[2])
            else:
                print("Неверный формат команды. Используйте: temp_range/temp_range_avg дата1 дата2")
        else:
            send_command_to_manager(command)
