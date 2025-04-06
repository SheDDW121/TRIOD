import math
import pika
import json
from datetime import datetime
import threading
from sortedcontainers import SortedDict

class Showcase:
    def __init__(self):
        self.accuracy = 3  # количество знаков после запятой, которые отдает клиенту
        self.data = SortedDict()  # Используем SortedDict для хранения данных
        self.lock = threading.Lock()  # для потокобезопасности
        self.connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        self.channel = self.connection.channel()
        
        # Очередь для получения новых данных от менеджера/хранителей
        self.channel.queue_declare(queue='showcase_data')
        self.channel.basic_consume(queue='showcase_data', 
                                  on_message_callback=self.process_new_data,
                                  auto_ack=True)
        
        # Очередь для запросов от клиента
        self.channel.queue_declare(queue='showcase_requests')
        self.channel.basic_consume(queue='showcase_requests',
                                  on_message_callback=self.process_request,
                                  auto_ack=True)
        
        print("Витрина данных запущена и ожидает сообщений...")

    def process_new_data(self, ch, method, properties, body):
        """Обработка новых данных от менеджера/хранителя"""
        try:
            request = json.loads(body)
            row = request['data']
            date_str = row['date_parsed']  # Дата гарантированно в формате '%d-%m-%Y'
            date = datetime.strptime(date_str, '%d-%m-%Y')  # Преобразуем дату в datetime

            # Возможные имена столбцов с датой
            possible_date_columns = ['temp_max', ' _tempm', 'Data.Temperature.Avg Temp', 'BASEL_temp_mean']

            # Словарь для ассоциации столбцов с числовыми значениями
            column_to_number = {
                'temp_max': 0,
                ' _tempm': 1,
                'Data.Temperature.Avg Temp': 2,
                'BASEL_temp_mean': 3
            }

            # Проверяем, какой из столбцов присутствует в DataFrame и ассоциируем его с числом
            column_number = None

            for column in possible_date_columns:
                if column in row:  # Проверяем, есть ли столбец как ключ в словаре row
                    column_number = column_to_number[column]
                    break

            count_to_add = 1
            
            if column_number == 0:
                # Среднее между минимальной и максимальной температурами
                temperature = (float(row['temp_min']) + float(row['temp_max'])) / 2
            elif column_number == 1:
                # Используем значение _tempm
                temperature = float(row[' _tempm'])
            elif column_number == 2:
                # Используем значение Data.Temperature.Avg Temp
                temperature = float(row['Data.Temperature.Avg Temp'])
            else:
                # Среднее из всех столбцов с суффиксом _temp_mean
                temp_mean_columns = [col for col in row.keys() if col.endswith('_temp_mean')]
                if temp_mean_columns:
                    temp_sum = sum(float(row[col]) for col in temp_mean_columns if row[col] not in [None, ''])
                    temp_count = sum(1 for col in temp_mean_columns if row[col] not in [None, ''])
                    count_to_add = temp_count
                    
                    if temp_count > 0:
                        temperature = temp_sum / temp_count
                    else:
                        print(f"Нет допустимых значений температуры в строке: {row}")
                        return  # Пропускаем эту запись
                else:
                    print(f"Не найдены столбцы с суффиксом _temp_mean в строке: {row}")
                    return  # Пропускаем эту запись
                
            if math.isnan(temperature):
                return
            
            # оперируем данными непосредственно из словаря витрины, так что навешиваем замок для потокобезопасности
            with self.lock:
                if date in self.data:
                    # Обновляем среднюю температуру
                    old_avg, old_count = self.data[date]
                    new_count = old_count + count_to_add
                    
                    new_avg = (old_avg * old_count + temperature * count_to_add) / new_count
                    self.data[date] = (new_avg, new_count)
                else:
                    # Добавляем новую запись
                    self.data[date] = (temperature, count_to_add)

        except Exception as e:
            print(f"[Ошибка] Не удалось загрузить данные в витрину: {e}")
            result = {'status': '500', 'from': "showcaseX", 'message': str(e)}
            self.send_response('client_responses', result)
    
    def process_request(self, ch, method, properties, body):
        """Обработка запросов от клиента"""
        try:
            request = json.loads(body)
            command = request.get('command')
            date1 = request.get('date1')
            date2 = request.get('date2')
            
            if command == 'temp_range':
                result = self.get_temp_range(date1, date2)
                self.send_response(request['reply_to'], result)

            elif command == 'temp_range_avg':
                result = self.get_temp_range_avg(date1, date2)
                self.send_response(request['reply_to'], result)

        except Exception as e:
            print(f"[Ошибка] не удалось обработать запрос от клиента: {e}")
            result = {'status': '500', 'from': "showcaseX", 'message': str(e)}
            self.send_response(request['reply_to'], result)
    
    def get_temp_range(self, start_date, end_date):
        """Получение данных о температуре за указанный период"""

        # Преобразуем в объекты datetime для сравнения
        start = datetime.strptime(start_date, '%d-%m-%Y')
        end = datetime.strptime(end_date, '%d-%m-%Y')

        result = {}
        exists = False  # Флаг для отслеживания, вошел ли цикл хотя бы один раз
        found = False
        
        with self.lock:
            # Использование irange для получения диапазона дат
            for date in self.data.irange(start, end):

                exists = True
                avg_temp, _ = self.data[date]

                # преобразуем datetime обратно в строку нужного формата для отправки ответа клиенту
                date_str = date.strftime('%d-%m-%Y')

                result[date_str] = round(avg_temp, self.accuracy)
                found = True

        response = {'status': 'success', 'data': result, 'from': "showcase1"}
        if not exists:
            response['status'] = "204"  # No Content
        elif not found:
            response['status'] = "404"  # Not Found
        
        return response
    
    def get_temp_range_avg(self, start_date, end_date):
        """Получение средней температуры за указанный период"""
        temp_range = self.get_temp_range(start_date, end_date)
        
        if temp_range['status'] != 'success':
            return temp_range
        
        data = temp_range['data']
        if not data:
            return {'status': 'error', 'message': 'Нет данных за указанный период'}
        
        total_temp = sum(data.values())
        avg_temp = round((total_temp / len(data)), self.accuracy)
        
        return {'status': 'success', 'avg_temperature': avg_temp, 'from': "showcase2"}
    
    def send_response(self, reply_to, response):
        """Отправка ответа клиенту"""
        self.channel.basic_publish(
            exchange='',
            routing_key=reply_to,
            body=json.dumps(response)
        )
    
    def run(self):
        """Запуск процесса витрины"""
        self.channel.start_consuming()

if __name__ == "__main__":
    showcase = Showcase()
    showcase.run()
