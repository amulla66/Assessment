CREATE TABLE attribution_modeling AS
WITH installs AS (
    SELECT 
        i.install_date,
        DATE_TRUNC('week', i.install_date) AS week,
        COALESCE(i.media_channel, 'organic') AS media_source,
        i.visitor_id,
        m.id AS user_id
    FROM installs_all i
    LEFT JOIN mapping m ON i.visitor_id = m.vst_id
),
installs_with_revenue AS (
    SELECT 
        inst.week,
        inst.media_source,
        inst.visitor_id,
        r.revenue::numeric
    FROM installs inst
    LEFT JOIN revenues r ON inst.user_id = r.user_id
),
weekly_costs AS (
    SELECT 
        DATE_TRUNC('week', date) AS week,
        media_channel AS media_source,
        SUM(spend)::numeric AS spend
    FROM costs
    GROUP BY 1, 2
),
weekly_metrics AS (
    SELECT 
        week,
        media_source,
        COUNT(DISTINCT visitor_id) AS installs,
        SUM(revenue) AS revenue
    FROM installs_with_revenue
    GROUP BY 1, 2
)
SELECT 
    m.week,
    m.media_source,
    m.installs,
    m.revenue,
    c.spend,
    ROUND((c.spend / NULLIF(m.installs, 0))::numeric, 2) AS CAC,
    ROUND((m.revenue / NULLIF(c.spend, 0))::numeric, 4) AS ROAS
FROM weekly_metrics m
LEFT JOIN weekly_costs c 
  ON m.week = c.week AND m.media_source = c.media_source;
