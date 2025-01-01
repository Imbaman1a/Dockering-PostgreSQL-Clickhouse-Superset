from clickhouse_driver import Client
from csv import DictReader

def iter_csv(filename, batch_size=50000):
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
                try_float(line['trip_distance']),
                try_float(line['pickup_longitude']),
                try_float(line['pickup_latitude']),
                int(line['RateCodeID']),
                line['store_and_fwd_flag'],
                try_float(line['dropoff_longitude']),
                try_float(line['dropoff_latitude']),
                int(line['payment_type']),
                try_float(line['fare_amount']),
                try_float(line['extra']),
                try_float(line['mta_tax']),
                try_float(line['tip_amount']),
                try_float(line['tolls_amount']),
                try_float(line['improvement_surcharge']),
                try_float(line['total_amount'])
            ))

            if len(batch) >= batch_size:
                yield batch
                batch = []
        
        if batch:  # Оставшиеся строки
            yield batch

def try_float(value):
    try:
        # Пробуем преобразовать строку в float
        return float(value) if value else None  # Возвращаем None, если строка пустая
    except ValueError:
        # Если не удалось преобразовать, возвращаем None
        return None

# Подключение к ClickHouse
client = Client(host='clickhouse',
                port=9000,
                user='username',
                password='password',
                database='my_database')

# Вставка данных
client.execute("""
CREATE TABLE IF NOT EXISTS nyc_taxi_csv (
    VendorID Int32,
    tpep_pickup_datetime String,
    tpep_dropoff_datetime String,
    passenger_count Nullable(Int32),
    trip_distance Nullable(Decimal(18, 2)),
    pickup_longitude Nullable(Float64),
    pickup_latitude Nullable(Float64),
    RateCodeID Nullable(Int32),
    store_and_fwd_flag String,
    dropoff_longitude Nullable(Float64),
    dropoff_latitude Nullable(Float64),
    payment_type Nullable(Int32),
    fare_amount Nullable(Decimal(18, 2)),
    extra Nullable(Float32),
    mta_tax Nullable(Decimal(18, 2)),
    tip_amount Nullable(Decimal(18, 2)),
    tolls_amount Nullable(Decimal(18, 2)),
    improvement_surcharge Nullable(Decimal(18, 2)),
    total_amount Nullable(Decimal(18, 2))
) ENGINE = MergeTree()
ORDER BY VendorID;
""")

for batch in iter_csv('temp.csv'):
    client.execute('INSERT INTO nyc_taxi_csv VALUES', batch)

