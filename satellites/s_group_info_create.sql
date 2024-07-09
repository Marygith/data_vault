CREATE TABLE IF NOT EXISTS dds.s_group_info (
    hk_group_id INT,
    title VARCHAR(5000),
    status VARCHAR(100),
    source VARCHAR(100),
    load_date TIMESTAMP,
    PRIMARY KEY (hk_group_id)
);