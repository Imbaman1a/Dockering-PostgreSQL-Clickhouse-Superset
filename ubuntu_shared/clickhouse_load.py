from clickhouse_connect import get_client
import csv
client = get_client(host='clickhouse',
port=8123, 
username='username',
password='password',
database='my_database')
print(1)
create_table = """
CREATE TABLE IF NOT EXISTS nyc_taxi_clickhouse (
VendorID Int32,
tpep_pickup_datetime String,
tpep_dropoff_datetime String,
passenger_count Int32,
trip_distance Float32,
pickup_longitude Float64,
pickup_latitude Float64,
RateCodeID Int32,
store_and_fwd_flag String,
dropoff_longitude Float64,
dropoff_latitude Float64,
payment_type Int32,
fare_amount Float32,
extra Float32,
mta_tax	Float32,
tip_amount Float32,
tolls_amount Float32,
improvement_surcharge Float32,
total_amount Float32 
) ENGINE = MergeTree()
ORDER BY VendorID; """
client.command(create_table)
checking = """
DESCRIBE TABLE nyc_taxi_clickhouse;
"""
batch_size = 100000
data_batch = []
uploaded = 0
with open('yellow_tripdata_2015-01.csv', 'r') as f:
    csv_reader = csv.reader(f)
    for row in csv_reader:
        data_batch.append(row)
        if len(data_batch) >= batch_size:

            client.insert('nyc_taxi_clickhouse', data=data_batch )
            data_batch.clear()
            uploaded += batch_size
            print(uploaded)
    if data_batch:
        client.insert('nyc_taxi_clickhouse', data=data_batch)
 
client.disconnect()
