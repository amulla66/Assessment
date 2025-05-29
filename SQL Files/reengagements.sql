
CREATE MATERIALIZED VIEW reengagements AS
WITH combined_installs AS (
    SELECT 
        m.id AS user_id,
        i.install_date::timestamp AS install_time,
        COALESCE(i.media_channel, 'organic') AS media_source
    FROM installs_all i
    JOIN mapping m ON i.visitor_id = m.vst_id
)
SELECT
    user_id,
    install_time,
    media_source,
    LAG(install_time) OVER (PARTITION BY user_id ORDER BY install_time) AS previous_install,
    CASE
        WHEN install_time - LAG(install_time) OVER (PARTITION BY user_id ORDER BY install_time) > INTERVAL '30 days'
        THEN TRUE ELSE FALSE
    END AS is_reengagement
FROM combined_installs;



CREATE OR REPLACE VIEW reengagement_summary AS
SELECT
    media_source,
    COUNT(*) FILTER (WHERE is_reengagement) AS reengaged_users,
    COUNT(*) AS total_installs,
    COUNT(DISTINCT user_id) AS unique_users,
    ROUND(
        COUNT(*) FILTER (WHERE is_reengagement)::NUMERIC / NULLIF(COUNT(*), 0),
        2
    ) AS reengagement_rate
FROM reengagements
GROUP BY media_source
ORDER BY media_source;



CREATE OR REPLACE VIEW reengaged_user_ltv AS
SELECT
    r.user_id,
    r.media_source,
    l.ltv
FROM reengagements r
JOIN user_ltv l ON r.user_id = l.user_id
WHERE is_reengagement = TRUE;


SELECT COUNT(DISTINCT user_id) AS total_reengaged_users
FROM reengagements
WHERE is_reengagement = TRUE;

