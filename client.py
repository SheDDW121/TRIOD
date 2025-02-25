import threading
import pika
import json
import time

from config import num_storages, num_vnodes, print_each_step, durability

def send_command(command):
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    
    channel.queue_declare(queue='manager_commands', durable=durability)
    channel.queue_declare(queue='client_responses', durable=durability)
    
    request = {'command': command, 'reply_to': 'client_responses'}
    channel.basic_publish(exchange='', routing_key='manager_commands', body=json.dumps(request))
    print(f"[Клиент] Отправлена команда: {command}")
    
    connection.close()

def listen_responses():
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    
    channel.queue_declare(queue='client_responses', durable=durability)
    
    def callback(ch, method, properties, body):
        response = json.loads(body)
        print(f"[Клиент] Получен ответ: {response}")
        print("\r> ", end="", flush=True)  # Это вернёт курсор в начало строки
    
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
    
    print("[Клиент] Введите команды: LOAD [файл], GET [дата], EXIT")
    while True:
        command = input("> ").strip()
        if command.upper() == "EXIT":
            print("[Клиент] Завершение работы.")
            break
        send_command(command)
    
    listen_responses()
