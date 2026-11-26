WITH username_check AS (
    SELECT EXISTS (
        SELECT 1 FROM users WHERE username = $1
    ) AS username_exists
), claimed_id AS (
    DELETE FROM available_user_ids
    WHERE user_id = (
        SELECT user_id FROM available_user_ids
        WHERE NOT (SELECT username_exists FROM username_check)
        ORDER BY user_id LIMIT 1 FOR UPDATE
    )
    RETURNING user_id
), inserted_user AS (
    INSERT INTO users (user_id, username, password_hash)
    OVERRIDING SYSTEM VALUE
    SELECT user_id, $1, $2 FROM claimed_id
    RETURNING user_id, username
), inserted_profile AS (
    INSERT INTO user_information (user_id, username, user_created)
    SELECT user_id, username, date_trunc('milliseconds', statement_timestamp())
    FROM inserted_user
    RETURNING user_id
)
SELECT username_exists, (SELECT user_id FROM inserted_profile)
FROM username_check
