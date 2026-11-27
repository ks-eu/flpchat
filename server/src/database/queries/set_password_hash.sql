UPDATE users
SET password_hash = $2
WHERE user_id = $1 AND password_hash = ''
RETURNING user_id
