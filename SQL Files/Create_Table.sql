-- costs
CREATE TABLE costs (
    date DATE,
    media_channel TEXT,
    operating_system TEXT,
    spend NUMERIC
);

-- installs
CREATE TABLE installs_google (
    install_time TIMESTAMP,
    media_channel TEXT,
    operating_system TEXT,
    visitor_id TEXT,
    attr_id TEXT
);

CREATE TABLE installs_organic (
    install_time TIMESTAMP,
    operating_system TEXT,
    visitor_id TEXT
);

CREATE TABLE installs_rest (
    install_time TIMESTAMP,
    media_channel TEXT,
    operating_system TEXT,
    visitor_id TEXT
);

-- mapping
CREATE TABLE mapping (
    id TEXT, -- user_id
    vst_id TEXT, -- visitor_id
    createdat TIMESTAMP
);

-- revenues
CREATE TABLE revenues (
    period DATE,
    user_id TEXT,
    revenue NUMERIC
);
