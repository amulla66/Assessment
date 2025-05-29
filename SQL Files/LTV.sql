-- Step 5A: Total revenue (LTV) per user
CREATE OR REPLACE VIEW user_ltv AS
SELECT 
    user_id, 
    SUM(revenue) AS ltv
FROM revenues
GROUP BY user_id;


CREATE MATERIALIZED VIEW ltv_segmented_users AS
WITH all_installs AS (
    SELECT visitor_id, install_time, media_channel
    FROM installs_google
    UNION ALL
    SELECT visitor_id, install_time, NULL AS media_channel
    FROM installs_organic
    UNION ALL
    SELECT visitor_id, install_time, media_channel
    FROM installs_rest
),
latest_installs AS (
    SELECT 
        visitor_id,
        MAX(install_time) AS latest_time
    FROM all_installs
    GROUP BY visitor_id
),
latest_user_installs AS (
    SELECT 
        a.visitor_id,
        a.install_time,
        COALESCE(a.media_channel, 'organic') AS media_source
    FROM all_installs a
    JOIN latest_installs l 
        ON a.visitor_id = l.visitor_id AND a.install_time = l.latest_time
),
mapped_users AS (
    SELECT 
        m.id AS user_id,
        i.media_source
    FROM mapping m
    JOIN latest_user_installs i 
        ON m.vst_id = i.visitor_id
)
SELECT
    l.user_id,
    l.ltv,
    CASE
        WHEN l.ltv >= (
            SELECT PERCENTILE_CONT(0.66) 
            WITHIN GROUP (ORDER BY ltv) 
            FROM user_ltv
        ) THEN 'High'
        WHEN l.ltv >= (
            SELECT PERCENTILE_CONT(0.33) 
            WITHIN GROUP (ORDER BY ltv) 
            FROM user_ltv
        ) THEN 'Medium'
        ELSE 'Low'
    END AS ltv_tier,
    mu.media_source
FROM user_ltv l
JOIN mapped_users mu 
    ON l.user_id = mu.user_id;

        

-- Step 5C: Count users in each LTV tier by media source
CREATE OR REPLACE VIEW ltv_distribution_by_source AS
SELECT
    ltv_tier,
    media_source,
    COUNT(*) AS user_count
FROM ltv_segmented_users
GROUP BY ltv_tier, media_source
ORDER BY ltv_tier, media_source;
