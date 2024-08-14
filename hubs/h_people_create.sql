CREATE TABLE IF NOT EXISTS dds.h_people (
    hk_person_id VARCHAR(5000),
    person_id INT,
    source VARCHAR(100),
    load_date TIMESTAMP,
    PRIMARY KEY (hk_person_id) ENABLED
);