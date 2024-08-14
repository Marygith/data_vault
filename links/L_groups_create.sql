CREATE TABLE IF NOT EXISTS dds.L_groups (
    hk_L_group_id VARCHAR(5000),
    hk_msg_id VARCHAR(5000),
    hk_group_id VARCHAR(5000),
    source VARCHAR(100),
    load_date TIMESTAMP,
    PRIMARY KEY (hk_L_group_id) ENABLED
);
