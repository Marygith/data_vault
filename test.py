# %%

import csv
import vertica_python

conn_info = {
    'host': 'vertica-ce',
    'port': '5433',
    'user': 'dbadmin',
    'database': 'VMart',
    'schema': 'public',
}

# %%

with vertica_python.connect(**conn_info) as connection:
    cursor = connection.cursor()
    with open('./dags/files/people.csv', 'r',encoding="cp1251") as f:
        reader = csv.reader(f)
        columns = next(reader)
        query = f"INSERT INTO stg.people ({','.join(columns)}) VALUES ({','.join(['%s'] * len(columns))})"
        for data in reader:
            cursor.execute(query, data)
        connection.commit()