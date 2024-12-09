CREATE TABLE IF NOT EXISTS dds.L_chat_owners (
    hk_L_owner_id VARCHAR(5000),
    hk_person_id VARCHAR(5000),
    hk_group_id VARCHAR(5000),
    source VARCHAR(100),
    load_date TIMESTAMP,
    PRIMARY KEY (hk_L_owner_id) ENABLED
);