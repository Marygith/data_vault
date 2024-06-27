CREATE TABLE h_people (
    hk_person_id INT,
    person_id INT,
    source TEXT,
    load_date TIMESTAMP,
    PRIMARY KEY (hk_person_id)
) DISTRIBUTED BY (hk_person_id);
