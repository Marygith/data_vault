CREATE TABLE IF NOT EXISTS stg.groups (
    id INT,
    owner_id INT,
    group_name VARCHAR(5000),
    created_date TIMESTAMP,
    PRIMARY KEY (id)
);