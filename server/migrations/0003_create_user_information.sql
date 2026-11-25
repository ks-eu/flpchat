ALTER TABLE users ADD UNIQUE (user_id, username);

CREATE TABLE user_information (
    user_id INTEGER PRIMARY KEY,
    username VARCHAR(32) NOT NULL,
    admin_status BOOLEAN NOT NULL DEFAULT FALSE,
    last_online TIMESTAMPTZ DEFAULT NULL,
    FOREIGN KEY (user_id, username)
        REFERENCES users (user_id, username) ON UPDATE CASCADE
);
