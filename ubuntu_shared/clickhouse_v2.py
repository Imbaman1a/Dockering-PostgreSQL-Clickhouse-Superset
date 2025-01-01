from clickhouse_driver import Client
from csv import DictReader

def iter_csv(filename, batch_size=10000):
    batch = []
    with open(filename, 'r') as f:
        reader = DictReader(f)
        for line in reader:
            # Преобразуем данные, если необходимо
            batch.append((
                int(line['VendorID']),
                line['tpep_pickup_datetime'],
                line['tpep_dropoff_datetime'],
                int(line['passenger_count']),
                float(line['trip_distance']),
                float(line['pickup_longitude']),
                float(line['pickup_latitude']),
                int(line['RateCodeID']),
                line['store_and_fwd_flag'],
                float(line['dropoff_longitude']),
                float(line['dropoff_latitude']),
                int(line['payment_type']),
                float(line['fare_amount']),
                float(line['extra']),
                float(line['mta_tax']),
                float(line['tip_amount']),
                float(line['tolls_amount']),
                float(line['improvement_surcharge']),
                float(line['total_amount'])
            ))

            if len(batch) >= batch_size:
                yield batch
                batch = []
        
        if batch:  # Оставшиеся строки
            yield batch

# Подключение к ClickHouse
client = Client(host='clickhouse',
                port=9000,
                user='username',
                password='password',
                database='my_database')

# Вставка данных
client.execute('CREATE TABLE IF NOT EXISTS nyc_taxi_csv ('
               'VendorID Int32, '
               'tpep_pickup_datetime String, '
               'tpep_dropoff_datetime String, '
               'passenger_count Int32, '
               'trip_distance Decimal, '
               'pickup_longitude Float64, '
               'pickup_latitude Float64, '
               'RateCodeID Int32, '
               'store_and_fwd_flag String, '
               'dropoff_longitude Float64, '
               'dropoff_latitude Float64, '
               'payment_type Int32, '
               'fare_amount Decimal, '
               'extra Float32, '
               'mta_tax Decimal, '
               'tip_amount Decimal, '
               'tolls_amount Decimal, '
               'improvement_surcharge Decimal, '
               'total_amount Decimal) '
               'ENGINE MergeTree() ORDER BY VendorID')

# Вставка данных в таблицу
for batch in iter_csv('temp.csv'):
    client.execute('INSERT INTO nyc_taxi_csv VALUES', batch)

