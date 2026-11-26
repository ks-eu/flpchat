WITH removed_user AS (
    DELETE FROM users WHERE user_id = $1 RETURNING user_id
)
INSERT INTO available_user_ids (user_id)
SELECT user_id FROM removed_user
