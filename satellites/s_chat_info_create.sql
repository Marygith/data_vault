CREATE TABLE s_chat_info (
    hk_msg_id INT,
    text_msg TEXT,
    msg_from INT,
    msg_to INT,
    status TEXT,
    source TEXT,
    load_date TIMESTAMP,
    PRIMARY KEY (hk_msg_id)
) DISTRIBUTED BY (hk_msg_id);
