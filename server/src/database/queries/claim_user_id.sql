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
)
SELECT CASE WHEN username_exists THEN EXISTS (SELECT 1 FROM available_user_ids)
            ELSE FALSE
       END AS duplicate_with_space,
       (SELECT user_id FROM claimed_id)
FROM username_check
