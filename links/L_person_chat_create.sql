CREATE TABLE L_person_chat (
    hk_L_person_chat INT,
    hk_person_id INT,
    hk_msg_id INT,
    source TEXT,
    load_date TIMESTAMP,
    PRIMARY KEY (hk_L_person_chat)
) DISTRIBUTED BY (hk_L_person_chat);
