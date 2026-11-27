DELETE FROM available_user_ids WHERE user_id BETWEEN 1 AND 50000;
INSERT INTO users (user_id, username, password_hash)
OVERRIDING SYSTEM VALUE
SELECT user_id,
       'bench-' || lpad(user_id::text, 5, '0'),
       CASE WHEN user_id = 1 THEN '$argon2id$v=19$m=19456,t=2,p=1$bGlnaHRjaGF0LWJlbmNobWFyaw$NG0zwI7kM/Cq+dLLB3HMbPwk1I8NW1iSDPpYPr/ImjU'
            ELSE 'fixture-hash' END
FROM generate_series(1, 50000) AS fixture(user_id);
INSERT INTO user_information (user_id, username)
SELECT user_id, username FROM users ORDER BY user_id;
INSERT INTO messages (message_id, user_id, message)
SELECT n, 1, 'target public ' || n
FROM generate_series(1, 10000) AS message(n);
INSERT INTO messages (message_id, user_id, message)
SELECT 10000 + n, 2 + ((n - 1) % 49999), 'public ' || n
FROM generate_series(1, 990000) AS message(n);
INSERT INTO private_messages (privatemessage_id, user_id, target_id, message)
SELECT n, 1, 2, 'target outgoing ' || n
FROM generate_series(1, 5000) AS message(n);
INSERT INTO private_messages (privatemessage_id, user_id, target_id, message)
SELECT 5000 + n, 2, 1, 'target incoming ' || n
FROM generate_series(1, 5000) AS message(n);
INSERT INTO private_messages (privatemessage_id, user_id, target_id, message)
SELECT 10000 + n,
       2 + ((n - 1) % 49999),
       2 + (n % 49999),
       'private ' || n
FROM generate_series(1, 490000) AS message(n);
