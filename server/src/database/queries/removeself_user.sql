WITH account AS (
    SELECT user_id, password_hash = $2 AS password_matches
    FROM users
    WHERE username = $1
    FOR UPDATE
), removed_user AS (
    DELETE FROM users
    WHERE user_id = (SELECT user_id FROM account WHERE password_matches)
    RETURNING user_id
)
SELECT (SELECT password_matches FROM account), (SELECT user_id FROM removed_user)
