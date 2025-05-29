CREATE TABLE installs_all AS
SELECT 
    install_time::date AS install_date,
    media_channel,
    visitor_id
FROM installs_google
UNION ALL
SELECT 
    install_time::date,
    NULL AS media_channel,
    visitor_id
FROM installs_organic
UNION ALL
SELECT 
    install_time::date,
    media_channel,
    visitor_id
FROM installs_rest;
