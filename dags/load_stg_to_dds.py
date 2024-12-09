import pendulum
import hashlib
from datetime import datetime
from airflow.decorators import dag, task
import vertica_python

vertica_conn_info = {
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
    tags=['load_stg_to_dds'],
    is_paused_upon_creation=False
)
def load_stg_to_dds():

    @task
    def load_data_dds():
        src_conn = vertica_python.connect(**vertica_conn_info)
        dds_conn = vertica_python.connect(**vertica_conn_info)

        with src_conn.cursor() as source_cursor, dds_conn.cursor() as target_cursor:

            # Заполнение s_person_info
            source_cursor.execute("SELECT * FROM stg.people")

            insert_s_person_info = """
                    INSERT INTO dds.s_person_info(hk_person_id, name, date_of_birthday, country, phone, email, source, load_date)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s) 
            """

            insert_h_people = """
                    INSERT INTO dds.h_people(hk_person_id, person_id, source, load_date)
                    VALUES (%s, %s, %s, %s)
            """

            for row in source_cursor.fetchall():
                hk_person_id = 'P' + str(row[0]) + '_' + 'TEST'
                target_cursor.execute(insert_s_person_info, (
                    hk_person_id, row[1], row[4], row[3], row[5], row[6], 'TEST', datetime.now()))
                target_cursor.execute(insert_h_people,(
                    hk_person_id, row[0], 'TEST', datetime.now()))

            # Заполнение s_group_info
            source_cursor.execute("SELECT * FROM stg.groups")

            insert_s_group_info = """
                    INSERT INTO dds.s_group_info(hk_group_id, title, status, source, load_date)
                    VALUES (%s, %s, %s, %s, %s)
                """
            
            insert_h_groups = """
                    INSERT INTO dds.h_groups(hk_group_id, group_id, created_date, source, load_date)
                    VALUES (%s, %s, %s, %s, %s)
                """

            for row in source_cursor.fetchall():
                hk_group_id = 'G' + str(row[0]) + '_' + 'TEST'
                target_cursor.execute(insert_s_group_info,
                    (hk_group_id, row[2], 'open', 'TEST', datetime.now()))
                target_cursor.execute(insert_h_groups,
                    (hk_group_id, row[0], row[3], 'TEST', datetime.now()))

            # Заполнение s_chat_info
            source_cursor.execute("SELECT * FROM stg.chats")

            insert_s_chat_info = """
                    INSERT INTO dds.s_chat_info(hk_msg_id, text_msg, msg_from, msg_to, status, source, load_date)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """
            
            insert_h_chats = """
                    INSERT INTO dds.h_chats(hk_msg_id, msg_id, msg_date, msg_time, source, load_date)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """


            for row in source_cursor.fetchall():
                hk_msg_id = 'C' + str(row[0]) + '_' + 'TEST'
                target_cursor.execute(insert_s_chat_info,
                            (hk_msg_id, row[5], row[3], row[4], 'success', 'TEST', datetime.now()))
                target_cursor.execute(insert_h_chats,
                            (hk_msg_id, row[0], row[1], row[2], 'TEST', datetime.now()))


            # # # Заполнение l_groups
            source_cursor.execute('''
                select distinct hc.hk_msg_id, hg.hk_group_id  
                from stg.chats c 
                inner join dds.h_chats hc 
                    on hc.msg_id = c.msg_id 
                inner join dds.h_groups hg 
                    on hg.group_id = c.msg_group_id

            ''') 

            insert_l_groups = """
                    INSERT INTO dds.L_groups(hk_L_group_id, hk_msg_id, hk_group_id, source, load_date)
                    VALUES (%s, %s, %s, %s, %s)
                """

            for row in source_cursor.fetchall():
                hk_L_group_id = row[0] + '_' + row[1]
                target_cursor.execute(insert_l_groups,
                    (hk_L_group_id, row[0], row[1], 'TEST', datetime.now()))

            # Заполнение l_chat_owners
            source_cursor.execute('''
                select distinct g.owner_id, hg.hk_group_id  
                from stg.groups g 
                inner join dds.h_groups hg 
                    on hg.group_id = g.id 
            ''')

            insert_l_chat_owners = """
                INSERT INTO dds.L_chat_owners(hk_L_owner_id, hk_person_id, hk_group_id, source, load_date)
                VALUES (%s, %s, %s, %s, %s)
            """

            for row in source_cursor.fetchall():
                hk_owner_id = 'P' + str(row[0]) + '_' + 'TEST' + '_' + row[1]
                target_cursor.execute(insert_l_chat_owners,
                    (hk_owner_id, 'P' + str(row[0]) + '_' + 'TEST', row[1], 'TEST', datetime.now()))


        #     # # Заполнение l_person_chat (какие люди в каких есть чатах)
            
            source_cursor.execute(
                '''
                    select distinct hp.hk_person_id, sci.hk_msg_id 
                    from dds.s_chat_info sci 
                    inner join dds.h_people hp 
                        on sci.msg_from = hp.person_id 
                    union 
                    select distinct hp.hk_person_id, sci.hk_msg_id 
                    from dds.s_chat_info sci 
                    inner join dds.h_people hp 
                        on sci.msg_to = hp.person_id 
                    --limit 700
                '''
            )

            insert_l_person_chat = """
                    INSERT INTO dds.L_person_chat(hk_L_person_chat, hk_person_id, hk_msg_id, source, load_date)
                    VALUES (%s, %s, %s, %s, %s)
            """

            for row in source_cursor.fetchall():
                hk_l_person_chat = row[0] + '_' + row[1]
                target_cursor.execute(insert_l_person_chat,
                    (hk_l_person_chat, row[0], row[1], 'TEST', datetime.now())
                )

        src_conn.commit()
        dds_conn.commit()
        src_conn.close()
        dds_conn.close()

    (
        load_data_dds()
    )


load_etl = load_stg_to_dds()
