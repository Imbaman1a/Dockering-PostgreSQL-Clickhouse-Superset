from clickhouse_driver import Client
from csv import DictReader
from datetime import datetime

def iter_csv(filename):
    with open(filename, 'r') as f:
        converters = {
            'qty':int
        }
        reader = DictReader(f)
        for line in reader:
            yield {k:(converters[k](v) if k in converters else v) for k, v in line.items()}


client = Client(host='clickhouse',
port=9000,
user='username',
password='password',
database='my_database')

print(client.execute('SELECT * FROM nyc_taxi_clickhouse LIMIT 5;'))

client.execute('CREATE TABLE IF NOT EXISTS nyc_taxi_csv (VendorID Int32,tpep_pickup_datetime String,tpep_dropoff_datetime String,passenger_count Int32,trip_distance Decimal,pickup_longitude Float64,pickup_latitude Float64,RateCodeID Int32,store_and_fwd_flag String,dropoff_longitude Float64,dropoff_latitude Float64,payment_type Int32,fare_amount Decimal,extra Float32,mta_tax Decimal,tip_amount Decimal,tolls_amount Decimal,improvement_surcharge Decimal,total_amount Decimal)  ENGINE MergeTree() ORDER BY VendorID')

print(1)
client.execute('INSERT INTO nyc_taxi_csv VALUES', iter_csv('temp.csv'))
