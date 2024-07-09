CREATE TABLE IF NOT EXISTS dds.h_chats (
    hk_msg_id INT,
    msg_id INT,
    msg_time TIMESTAMP,
    source VARCHAR(100),
    load_date TIMESTAMP,
    PRIMARY KEY (hk_msg_id)
);
