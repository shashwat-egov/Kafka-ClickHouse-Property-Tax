CREATE TABLE IF NOT EXISTS punjab_property_tax.dimension_tenant_id
(
    -- created time 
    created_time DateTime64(3) DEFAULT now64(3),

    -- dimension columns
    code String,
    locale LowCardinality(String),
    value String

)
ENGINE = ReplacingMergeTree(created_time)
ORDER BY (locale, code)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS punjab_property_tax.dimension_property_type
(
    -- created time 
    created_time DateTime64(3) DEFAULT now64(3),

    -- dimension columns
    code String,
    locale LowCardinality(String),
    value String

)
ENGINE = ReplacingMergeTree(created_time)
ORDER BY (locale, code)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS punjab_property_tax.dimension_usage_category
(
    -- created time 
    created_time DateTime64(3) DEFAULT now64(3),

    -- dimension columns
    code String,
    locale LowCardinality(String),
    value String

)
ENGINE = ReplacingMergeTree(created_time)
ORDER BY (locale, code)
SETTINGS index_granularity = 8192;



CREATE TABLE IF NOT EXISTS punjab_property_tax.dimension_ownership_category
(
    -- created time 
    created_time DateTime64(3) DEFAULT now64(3),

    -- dimension columns
    code String,
    locale LowCardinality(String),
    value String

)
ENGINE = ReplacingMergeTree(created_time)
ORDER BY (locale, code)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS punjab_property_tax.dimension_payment_mode
(
    -- created time 
    created_time DateTime64(3) DEFAULT now64(3),

    -- dimension columns
    code String,
    locale LowCardinality(String),
    value String

)
ENGINE = ReplacingMergeTree(created_time)
ORDER BY (locale, code)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS punjab_property_tax.dimension_locality
(
    -- created time 
    created_time DateTime64(3) DEFAULT now64(3),

    -- dimension columns
    code String,
    locale LowCardinality(String),
    value String

)
ENGINE = ReplacingMergeTree(created_time)
ORDER BY (locale, code)
SETTINGS index_granularity = 8192;


CREATE TABLE IF NOT EXISTS punjab_property_tax.dimension_zone
(
    -- created time 
    created_time DateTime64(3) DEFAULT now64(3),

    -- dimension columns
    code String,
    locale LowCardinality(String),
    value String

)
ENGINE = ReplacingMergeTree(created_time)
ORDER BY (locale, code)
SETTINGS index_granularity = 8192;


CREATE TABLE IF NOT EXISTS punjab_property_tax.dimension_block
(
    -- created time 
    created_time DateTime64(3) DEFAULT now64(3),

    -- dimension columns
    code String,
    locale LowCardinality(String),
    value String

)
ENGINE = ReplacingMergeTree(created_time)
ORDER BY (locale, code)
SETTINGS index_granularity = 8192;