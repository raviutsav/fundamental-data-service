-- 1. Create ENUM type for period_type
CREATE TYPE period_type AS ENUM (
    'annuals',
    'quarterly'
);

-- 2. Create table with ON DELETE CASCADE
CREATE TABLE fundamental_data (
    id BIGSERIAL PRIMARY KEY NOT NULL,
    ticker VARCHAR(30) NOT NULL,
    period period_type NOT NULL,
    year VARCHAR(5) NOT NULL,
    month VARCHAR(3) NOT NULL,
    fundamental_data_type_id BIGINT NOT NULL
        REFERENCES fundamental_data_type(id)
        ON DELETE CASCADE,
    value NUMERIC NOT NULL,

    -- Unique constraint to prevent duplicates
    CONSTRAINT fundamental_data_unique UNIQUE (
        ticker,
        period,
        year,
        month,
        fundamental_data_type_id
    )
);

-- 3. Indexes
-- Fast lookup by ticker
CREATE INDEX idx_fundamental_data_ticker
    ON fundamental_data (ticker);

-- Fast lookup by ticker + time period (common financial queries)
CREATE INDEX idx_fundamental_data_ticker_period_year
    ON fundamental_data (ticker, period, year);

-- Improve join performance on FK
CREATE INDEX idx_fundamental_data_type_id
    ON fundamental_data (fundamental_data_type_id);
