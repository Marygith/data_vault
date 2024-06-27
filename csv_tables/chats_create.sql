CREATE TABLE chats (
    msg_id INT ,
    msg_time TIMESTAMP ,
    msg_from INT ,
    msg_to INT ,
    text_message TEXT ,
    msg_group_id INT ,
    PRIMARY KEY (msg_id)
);