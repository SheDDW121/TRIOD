import math
import pika
import json
from datetime import datetime
import threading

class Showcase:
    def __init__(self):
        self.accuracy = 3 # количество знаков после запятой, которые отдает клиенту, но на витрине хранится точное значение

        self.data = {}  # формат {дата: средняя_температура}
        self.lock = threading.Lock()  # для потокобезопасности
        self.connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        self.channel = self.connection.channel()
        
        # Очередь для получения новых данных от менеджера
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
        """Обработка новых данных от менеджера"""

        try:

            request = json.loads(body)

            row = request['data']
            date = row['date_parsed']

            # Возможные имена столбцов с датой
            # 1. temp_max + temp_min = seattle-weather.csv - we'll make the average of it
            # 2. _tempm = testset.csv
            # 3. Data.Temperature.Avg Temp = weather.csv
            # 4. BASEL_temp_mean + BUDAPEST_temp_mean + a lot of others = weather_prediction_dataset - we'll average the average of it
            possible_date_columns = ['temp_max',' _tempm', 'Data.Temperature.Avg Temp', 'BASEL_temp_mean']

            # Словарь для ассоциации столбцов с числовыми значениями
            column_to_number = {
                'temp_max': 0,
                ' _tempm': 1,
                'Data.Temperature.Avg Temp': 2,
                'BASEL_temp_mean': 3
            }

            # Проверяем, какой из столбцов присутствует в DataFrame и ассоциируем его с числом
            date_column = None
            column_number = None

            for column in possible_date_columns:
                if column in row:  # Проверяем, есть ли столбец как ключ в словаре row
                    date_column = column
                    column_number = column_to_number[column]
                    break

            with self.lock:
                count_to_add = 1
                
                if column_number == 0:
                    # Первый файл: среднее между минимальной и максимальной температурой
                    temperature = (float(row['temp_min']) + float(row['temp_max'])) / 2
                elif column_number == 1:
                    # Второй файл: используем значение _tempm
                    temperature = float(row[' _tempm'])
                elif column_number == 2:
                    # Третий файл: используем значение Data.Temperature.Avg Temp
                    temperature = float(row['Data.Temperature.Avg Temp'])
                else:
                    # Четвертый файл: среднее из всех столбцов с суффиксом _temp_mean
                    temp_mean_columns = [col for col in row.keys() if col.endswith('_temp_mean')]
                    if temp_mean_columns:
                        # Суммируем все значения столбцов temp_mean и делим на их количество
                        temp_sum = sum(float(row[col]) for col in temp_mean_columns if row[col] not in [None, ''])
                        temp_count = sum(1 for col in temp_mean_columns if row[col] not in [None, ''])
                        count_to_add = temp_count
                        
                        if temp_count > 0:
                            temperature = temp_sum / temp_count
                        else:
                            # Если нет допустимых значений, устанавливаем значение по умолчанию или пропускаем
                            print(f"Нет допустимых значений температуры в строке: {row}")
                            return  # Пропускаем эту запись
                    else:
                        print(f"Не найдены столбцы с суффиксом _temp_mean в строке: {row}")
                        return  # Пропускаем эту запись
                    
                if math.isnan(temperature):
                    return
                
                if date in self.data:
                    # Обновляем среднюю температуру
                    old_avg = self.data[date]['avg']
                    old_count = self.data[date]['count']
                    new_count = old_count + count_to_add
                    
                    new_avg = (old_avg * old_count + temperature * count_to_add) / new_count
                    self.data[date] = {'avg': new_avg, 'count': new_count}
                else:
                    # Добавляем новую запись
                    self.data[date] = {'avg': temperature, 'count': count_to_add}

            # print(f"Добавлены новые данные в витрину: {len(data)} записей")
            
        except Exception as e:
            print(f"[Ошибка] Не удалось загрузить данные в витрину: {e}")
            result = {'status': '500', 'from': "showcaseX", 'message': str(e)}
            self.send_response('client_responses', result)
            # return {"status": "ERROR", "message": f"Не удалось загрузить данные в витрину: {e}"}
    
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
        
        result = {}
        with self.lock:
            for date_str, data in self.data.items():
                date = datetime.strptime(date_str, '%d-%m-%Y')
                exists = True
                if start <= date <= end:
                    result[date_str] = round(data['avg'], self.accuracy)
                    found = True

        response = {'status': 'success', 'data': result, 'from': "showcase1"}
        if exists == False:
            response['status'] = "204"
        elif found == False:
            response['status'] = "404"
        
        return response
    
    def get_temp_range_avg(self, start_date, end_date):
        """Получение средней температуры за указанный период"""
        temp_range = self.get_temp_range(start_date, end_date)
        
        if temp_range['status'] != 'success':
            return temp_range
        
        data = temp_range['data']
        if not data:
            return {'status': 'error', 'message': 'Нет данных за указанный период'}
        
        # # Фильтруем значения NaN
        # filtered_data = {key: value for key, value in data.items() if value is not None and not math.isnan(value)}
        
        # # Если после фильтрации данных нет, возвращаем ошибку
        # if not filtered_data:
        #     return {'status': 'error', 'message': 'Нет данных за указанный период (без NaN)'}

        # # Считаем сумму и длину отфильтрованных данных
        # total_temp = sum(filtered_data.values())
        # avg_temp = round((total_temp / len(filtered_data)), self.accuracy)
        
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
