CREATE TABLE s_person_info (
    hk_person_id INT,
    name TEXT,
    date_of_birthday DATE,
    country TEXT,
    phone TEXT,
    email TEXT,
    source TEXT,
    load_date TIMESTAMP,
    PRIMARY KEY (hk_person_id)
) DISTRIBUTED BY (hk_person_id);
