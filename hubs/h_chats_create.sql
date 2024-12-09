CREATE TABLE IF NOT EXISTS dds.h_chats (
    hk_msg_id VARCHAR(5000),
    msg_id INT,
    msg_date DATE,
    msg_time TIME,
    source VARCHAR(100),
    load_date TIMESTAMP,
    PRIMARY KEY (hk_msg_id) ENABLED
);