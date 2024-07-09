import pendulum
import csv
from airflow.decorators import dag, task
import vertica_python

conn_info = {
    'host': 'vertica-ce',
    'port': '5433',
    'user': 'dbadmin',
    'database': 'VMart',
    'schema': 'public',
}


@dag(
    schedule_interval='@once',
    start_date=pendulum.now(),
    catchup=False,
    tags=['file_stg'],
    is_paused_upon_creation=False
)
def task2_file_stg():
    def load_csv_to_vertica(file_path, table_name):
        with vertica_python.connect(**conn_info) as connection:
            cursor = connection.cursor()
            with open(file_path, 'r',encoding="cp1251") as f:
                reader = csv.reader(f)
                columns = next(reader)
                query = f"INSERT INTO stg.{table_name} ({','.join(columns)}) VALUES ({','.join(['%s'] * len(columns))})"
                for data in reader:
                    cursor.execute(query, data)
            connection.commit()

    def truncate(table_name):
        with vertica_python.connect(**conn_info) as connection:
            cursor = connection.cursor()
            truncate_query = f"TRUNCATE TABLE stg.{table_name}"
            cursor.execute(truncate_query)
            connection.commit()

    def create(file_name):
        with open(f'dags/csv_tables/{file_name}.sql', 'r') as f:
            query = f.read()
        with vertica_python.connect(**conn_info) as connection:
            cursor = connection.cursor()
            cursor.execute(query)
            connection.commit()

    @task
    def load_peoples():
        create('people_create')
        truncate("people")
        load_csv_to_vertica("dags/files/people.csv", "peoples")

    @task
    def load_chats():
        create('chats_create')
        truncate("chats")
        load_csv_to_vertica("dags/files/chats.csv", "chats")

    @task
    def load_groups():
        create('groups_create')
        truncate("groups")
        load_csv_to_vertica("dags/files/groups.csv", "groups")

    load_peoples() >> load_chats() >> load_groups()


file_to_stg = task2_file_stg()
