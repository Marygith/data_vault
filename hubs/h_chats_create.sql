CREATE TABLE h_chats (
    hk_msg_id INT,
    msg_id INT,
    msg_time TIMESTAMP,
    source TEXT,
    load_date TIMESTAMP,
    PRIMARY KEY (hk_msg_id)
) DISTRIBUTED BY (hk_msg_id);
