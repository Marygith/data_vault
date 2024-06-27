CREATE TABLE h_groups (
    hk_group_id INT,
    group_id INT,
    created_date TIMESTAMP,
    source TEXT,
    load_date TIMESTAMP,
    PRIMARY KEY (hk_group_id)
) DISTRIBUTED BY (hk_group_id);
