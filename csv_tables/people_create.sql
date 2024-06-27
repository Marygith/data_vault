CREATE TABLE people (
    id INT,
    name TEXT,
    registration_date TIMESTAMP,
    country TEXT,
    date_of_birthday DATE,
    phone TEXT,
    email TEXT,
    PRIMARY KEY (id)
) DISTRIBUTED BY (id);
