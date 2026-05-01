SELECT table_schema, table_name 
FROM information_schema.tables 
WHERE table_name = 'logs';


CREATE TABLE logs (
    user_id    INT,
    event      VARCHAR(50),
    event_time TIMESTAMP,
    value      VARCHAR(50) 
);

INSERT INTO logs VALUES
(1, 'page_view', '2026-05-29 10:00:00', NULL),
(1, 'template_selected', '2026-05-29 10:01:00', 'pop_art'),
(1, 'template_selected', '2026-05-29 10:02:00', 'pop_art'),
(1, 'template_selected', '2026-05-29 10:10:00', 'pop_art'), -- разрыв >5 мин, новая сессия
(2, 'template_selected', '2026-05-29 11:00:00', 'folk'),
(2, 'template_selected', '2026-05-29 11:03:00', 'folk'),
(2, 'template_selected', '2026-05-29 11:04:00', 'minimal');

WITH logs_with_prev AS (
    -- 1. Находим время предыдущего события для каждого пользователя
    SELECT 
        user_id, event, event_time, value,
        LAG(event_time) OVER (PARTITION BY user_id ORDER BY event_time) AS prev_time
    FROM logs
),
session_flags AS (
    -- 2. Отмечаем начало новой сессии (первое событие или разрыв > 5 мин)
    SELECT 
        *,
        CASE 
            WHEN prev_time IS NULL THEN 1
            WHEN event_time - prev_time > INTERVAL '5 minutes' THEN 1 
            ELSE 0 
        END AS is_new_session
    FROM logs_with_prev
),
sessionized AS (
    -- 3. Генерируем ID сессии через кумулятивную сумму флагов
    SELECT 
        *,
        SUM(is_new_session) OVER (
            PARTITION BY user_id 
            ORDER BY event_time 
            ROWS UNBOUNDED PRECEDING
        ) AS session_id
    FROM session_flags
),
template_sequences AS (
    -- 4. Фильтруем только выборы шаблонов и смотрим предыдущий шаблон в той же сессии
    SELECT 
        user_id, session_id, event_time, value AS template_name,
        LAG(value) OVER (PARTITION BY user_id, session_id ORDER BY event_time) AS prev_template
    FROM sessionized
    WHERE event = 'template_selected' 
      AND value IS NOT NULL
)
-- 5. Считаем шаблоны, применённые подряд 2+ раз, и выводим топ-5
SELECT 
    template_name,
    COUNT(*) AS consecutive_apps_count
FROM template_sequences
WHERE template_name = prev_template
GROUP BY template_name
ORDER BY consecutive_apps_count DESC
LIMIT 5;


