CREATE TABLE L_groups (
    hk_L_group_id INT,
    hk_msg_id INT,
    hk_group_id INT,
    source TEXT,
    load_date TIMESTAMP,
    PRIMARY KEY (hk_L_group_id)
) DISTRIBUTED BY (hk_L_group_id);
