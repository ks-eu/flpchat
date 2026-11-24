CREATE TABLE users (
    user_id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    username VARCHAR(32) NOT NULL UNIQUE,
    password_hash TEXT NOT NULL,

    CONSTRAINT user_id_limit
        CHECK (user_id BETWEEN 1 AND 65535)
);
