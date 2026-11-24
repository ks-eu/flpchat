CREATE TABLE available_user_ids (
    user_id INTEGER PRIMARY KEY,

    CONSTRAINT available_user_id_limit
        CHECK (user_id BETWEEN 1 AND 65535)
);

INSERT INTO available_user_ids (user_id)
SELECT generate_series(1, 65535);
