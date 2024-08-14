CREATE TABLE IF NOT EXISTS dds.s_chat_info (
    hk_msg_id VARCHAR(5000),
    text_msg VARCHAR(5000),
    msg_from INT,
    msg_to INT,
    status VARCHAR(100),
    source VARCHAR(100),
    load_date TIMESTAMP,
    PRIMARY KEY (hk_msg_id) ENABLED
);