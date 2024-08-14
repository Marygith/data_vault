import pendulum
from airflow.decorators import dag, task
import vertica_python

conn_info = {
    'host': 'vertica-ce',
    'port': '5433',
    'user': 'dbadmin',
    'database': 'VMart',
    'schema': 'public',
}



def create_view_age_distr(connection):
    query = """
        -- группировка людей по возрасту
        CREATE OR REPLACE VIEW dm.person_age_distribution AS
        SELECT 
                AGE_IN_YEARS(CURRENT_DATE, spi.date_of_birthday) AS age,
                COUNT(DISTINCT spi.hk_person_id) AS people_count
            FROM 
                dds.L_groups lg
            INNER JOIN 
                dds.L_person_chat lpc ON lg.hk_msg_id = lpc.hk_msg_id
            INNER JOIN 
                dds.s_person_info spi ON lpc.hk_person_id = spi.hk_person_id
            WHERE 
                lg.hk_group_id IN (
                    SELECT 
                        hk_group_id
                    FROM 
                        dds.L_groups lg
                    GROUP BY 
                        hk_group_id
                    ORDER BY 
                        COUNT(hk_msg_id) DESC
                    LIMIT 5
                )
            GROUP BY 
                age
            ORDER BY 
                age;
    """
    connection.cursor().execute(query)
    connection.commit()

@dag(
    schedule_interval='@once',
    start_date=pendulum.now(),
    catchup=False,
    tags=['load_dds_to_cdm'],
    is_paused_upon_creation=False
)
def load_dds_to_cdm():
    @task
    def get_user_age_groups():
        query_result = """        
            select * from user_age_distribution;
        """
        with vertica_python.connect(**conn_info) as connection:
            create_view_age_distr(connection)
            cursor = connection.cursor()
            cursor.execute(query_result)
            result = cursor.fetchall()
            print(f"RESULT -> {result}")
            return result

    get_user_age_groups()

migraion = load_dds_to_cdm()
