CREATE TABLE IF NOT EXISTS dds.s_person_info (
    hk_person_id INT,
    name VARCHAR(5000),
    date_of_birthday DATE,
    country VARCHAR(5000),
    phone VARCHAR(5000),
    email VARCHAR(5000),
    source VARCHAR(100),
    load_date TIMESTAMP,
    PRIMARY KEY (hk_person_id)
);