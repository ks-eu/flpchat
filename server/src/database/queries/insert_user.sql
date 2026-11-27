WITH inserted_user AS (
    INSERT INTO users (user_id, username, password_hash)
    OVERRIDING SYSTEM VALUE
    VALUES ($1, $2, '')
    RETURNING user_id, username
)
INSERT INTO user_information (user_id, username, user_created)
SELECT user_id, username, date_trunc('milliseconds', statement_timestamp())
FROM inserted_user
RETURNING user_id
