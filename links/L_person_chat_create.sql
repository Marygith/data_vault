CREATE TABLE IF NOT EXISTS dds.L_person_chat (
    hk_L_person_chat INT,
    hk_person_id INT,
    hk_msg_id INT,
    source VARCHAR(100),
    load_date TIMESTAMP,
    PRIMARY KEY (hk_L_person_chat)
);