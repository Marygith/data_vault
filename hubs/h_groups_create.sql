CREATE TABLE IF NOT EXISTS dds.h_groups (
    hk_group_id VARCHAR(5000),
    group_id INT,
    created_date TIMESTAMP,
    source VARCHAR(100),
    load_date TIMESTAMP,
    PRIMARY KEY (hk_group_id) ENABLED
);