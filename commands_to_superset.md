docker exec -it superset /bin/bash

superset db upgrade
superset fab create-admin
#entry requiring data
superset init
#installing psycopg2-binary
pip install psycopg2-binary
pip install clickhouse-connect
