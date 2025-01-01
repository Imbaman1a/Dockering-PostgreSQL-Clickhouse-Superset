import psycopg2

conn = psycopg2.connect(
    dbname='test_app',
    user='pos_admin',
    password='pos_admin',
    host='postgres',
    port='5432'
)

cur = conn.cursor()

with open('yellow_tripdata_2015-01.csv', 'r') as f:
    cur.copy_from(f, 'nyc_taxi', sep=',',null='')
 

conn.commit()
cur.close()
conn.close()

