CREATE TABLE messages (
    message_id NUMERIC PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users (user_id),
    message TEXT NOT NULL,

    CONSTRAINT message_id_limit CHECK (
        message_id BETWEEN 0 AND 18446744073709551615
        AND message_id = trunc(message_id)
    )
);
