CREATE TABLE private_messages (
    privatemessage_id NUMERIC PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users (user_id) ON DELETE CASCADE,
    target_id INTEGER NOT NULL REFERENCES users (user_id) ON DELETE CASCADE,
    message TEXT NOT NULL,

    CONSTRAINT privatemessage_id_limit CHECK (
        privatemessage_id BETWEEN 0 AND 18446744073709551615
        AND privatemessage_id = trunc(privatemessage_id)
    )
);

CREATE INDEX ON private_messages (user_id);
CREATE INDEX ON private_messages (target_id);
