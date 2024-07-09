CREATE TABLE IF NOT EXISTS stg.people (
    id INT,
    name VARCHAR(5000),
    registration_date TIMESTAMP,
    country VARCHAR(5000),
    date_of_birthday DATE,
    phone VARCHAR(5000),
    email VARCHAR(5000),
    PRIMARY KEY (id)
);
