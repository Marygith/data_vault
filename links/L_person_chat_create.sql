CREATE TABLE IF NOT EXISTS dds.L_person_chat (
    hk_L_person_chat VARCHAR(5000),
    hk_person_id VARCHAR(5000),
    hk_msg_id VARCHAR(5000),
    source VARCHAR(100),
    load_date TIMESTAMP,
    PRIMARY KEY (hk_L_person_chat) ENABLED
);
