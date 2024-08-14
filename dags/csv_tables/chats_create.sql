CREATE TABLE IF NOT EXISTS stg.chats (
    msg_id INT,
    msg_date DATE,
    msg_time TIME,
    msg_from INT,
    msg_to INT,
    text_message VARCHAR(5000),
    msg_group_id INT,
    PRIMARY KEY (msg_id)
);