CREATE TABLE s_group_info (
    hk_group_id INT,
    title TEXT,
    status TEXT,
    source TEXT,
    load_date TIMESTAMP,
    PRIMARY KEY (hk_group_id)
) DISTRIBUTED BY (hk_group_id);
