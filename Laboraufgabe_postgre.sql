-- PostgreSQL Script
-- Temporär Warnungen ausschalten (entspricht SET sql_notes = 0 in MySQL nicht direkt in PostgreSQL anwendbar)
SET client_min_messages TO WARNING;

-- dq_quartile
CREATE TABLE IF NOT EXISTS dq_t_quartile (
  id SERIAL PRIMARY KEY,
  database_name VARCHAR(255) NOT NULL,
  schemata_name VARCHAR(255) NOT NULL,
  table_name VARCHAR(255) NOT NULL,
  column_name VARCHAR(255) NOT NULL,
  minimum DECIMAL(14, 4) NULL,
  Q1 DECIMAL(14, 4) NULL,
  Q2 DECIMAL(14, 4) NULL,
  Q3 DECIMAL(14, 4) NULL,
  maximum DECIMAL(14, 4) NULL,
  time TIMESTAMP DEFAULT NOW()
);

DROP FUNCTION IF EXISTS dq_quartile_entry(VARCHAR, VARCHAR, VARCHAR, VARCHAR);
CREATE OR REPLACE FUNCTION dq_quartile_entry(pDatabase varchar(255), pSchemata varchar(255), pTable varchar(255), pColumn varchar(255))
RETURNS VOID AS $$
BEGIN
  EXECUTE format(
    'INSERT INTO dq_t_quartile (database_name, schemata_name, table_name, column_name, minimum, Q1, Q2, Q3, maximum)
    WITH statistic AS (
      SELECT DISTINCT %I,
      NTILE(4) OVER (ORDER BY %I) AS quartile
      FROM %I.%I.%I
    )
    SELECT %L, %L, %L, %L,
    MIN(%I) AS "min",
    (SELECT MAX(statistic.%I) FROM statistic WHERE quartile = 1) AS "Q1",
    (SELECT MAX(statistic.%I) FROM statistic WHERE quartile = 2) AS "Q2",
    (SELECT MAX(statistic.%I) FROM statistic WHERE quartile = 3) AS "Q3",
    MAX(%I) AS "max"
    FROM statistic', pColumn, pColumn, pDatabase, pSchemata, pTable, pDatabase, pSchemata, pTable, pColumn, 
    pColumn, pColumn, pColumn, pColumn, pColumn);
END;
$$ LANGUAGE plpgsql;

DROP FUNCTION IF EXISTS dq_quartile(VARCHAR, VARCHAR, VARCHAR, VARCHAR);
CREATE OR REPLACE FUNCTION public.dq_quartile(pDatabase varchar(255), pSchemata varchar(255), pTable varchar(255), pColumn varchar(255))
RETURNS TABLE (min_value NUMERIC, Q1 NUMERIC, Q2 NUMERIC, Q3 NUMERIC, max_value NUMERIC)
AS $$
BEGIN
    RETURN QUERY EXECUTE format('
        with statistic as (
            SELECT DISTINCT %I, NTILE(4) OVER(ORDER BY %I) as quartile
            FROM %I.%I.%I
        )
        SELECT
            min(%I)::numeric as min_value,
            (SELECT max(statistic.%I) FROM statistic WHERE quartile = 1)::numeric as Q1,
            (SELECT max(statistic.%I) FROM statistic WHERE quartile = 2)::numeric as Q2,
            (SELECT max(statistic.%I) FROM statistic WHERE quartile = 3)::numeric as Q3,
            max(%I)::numeric as max_value
        FROM statistic', pColumn, pColumn, pDatabase, pSchemata, pTable, pColumn, pColumn, pColumn, pColumn, pColumn);
END;
$$ LANGUAGE plpgsql;

-- dq_duplicates
DROP FUNCTION IF EXISTS dq_duplicates(VARCHAR, VARCHAR, VARCHAR, VARCHAR);
CREATE OR REPLACE FUNCTION dq_duplicates(pDatabase varchar(255), pSchemata varchar(255), pTable VARCHAR, pColumn VARCHAR)
RETURNS INTEGER AS $$
DECLARE
  zahl_duplikate INTEGER;
BEGIN
  EXECUTE format('SELECT COUNT(%I) - COUNT(DISTINCT %I) AS zahl_duplikate FROM %I.%I.%I', pColumn, pColumn, pDatabase, pSchemata, pTable)
  INTO zahl_duplikate;
  RETURN zahl_duplikate;
END;
$$ LANGUAGE plpgsql;

-- dq_summary
CREATE TABLE IF NOT EXISTS dq_t_summary (
  id SERIAL PRIMARY KEY,
  database_name VARCHAR(255) NOT NULL,
  schemata_name VARCHAR(255) NOT NULL,
  table_name VARCHAR(255) NOT NULL,
  column_name VARCHAR(255) NOT NULL,
  count_value INT NULL,
  cardinality_value INT NULL,
  minimum DECIMAL(14, 4) NULL,
  maximum DECIMAL(14, 4) NULL,
  span DECIMAL(14, 4) NULL,
  average DECIMAL(14, 4) NULL,
  standard_deviation DECIMAL(14, 4) NULL,
  time TIMESTAMP DEFAULT NOW()
);

DROP FUNCTION IF EXISTS dq_summary_entry(VARCHAR, VARCHAR, VARCHAR, VARCHAR);
CREATE OR REPLACE FUNCTION dq_summary_entry(pDatabase VARCHAR, pSchemata VARCHAR, pTable VARCHAR, pColumn VARCHAR)
RETURNS VOID AS $$
BEGIN
  EXECUTE format('
    INSERT INTO dq_t_summary 
    (database_name, schemata_name, table_name, column_name, count_value, cardinality_value, minimum, maximum, span, average, standard_deviation)
    SELECT %L, %L, %L, %L, 
    COUNT(%I), 
    COUNT(DISTINCT %I), 
    MIN(%I), 
    MAX(%I), 
    MAX(%I) - MIN(%I), 
    AVG(%I), 
    STDDEV(%I)
    FROM %I.%I.%I', pDatabase, pSchemata, pTable, pColumn, 
    pColumn, pColumn, pColumn, pColumn, pColumn, pColumn, pColumn, pColumn, pDatabase, pSchemata, pTable);
END;
$$ LANGUAGE plpgsql;

DROP FUNCTION IF EXISTS dq_summary(VARCHAR, VARCHAR, VARCHAR, VARCHAR);
CREATE OR REPLACE FUNCTION dq_summary(pDatabase VARCHAR, pSchemata VARCHAR, pTable VARCHAR, pColumn VARCHAR)
RETURNS TABLE(anzahl INTEGER, cardinality INTEGER, minimum DECIMAL, maximum DECIMAL, span DECIMAL, average DECIMAL, standard_deviation DECIMAL) AS $$
BEGIN
  RETURN QUERY EXECUTE format('
    SELECT 
    COUNT(%I)::integer AS anzahl, 
    COUNT(DISTINCT %I)::integer AS cardinality, 
    MIN(%I)::decimal AS minimum, 
    MAX(%I)::decimal AS maximum, 
    MAX(%I)::decimal - MIN(%I)::decimal AS span, 
    AVG(%I)::decimal AS average, 
    STDDEV(%I)::decimal AS standard_deviation
    FROM %I.%I.%I', pColumn, pColumn, pColumn, pColumn, pColumn, pColumn, pColumn, pColumn, pDatabase, pSchemata, pTable);
END;
$$ LANGUAGE plpgsql;

-- dq_percentile
DROP FUNCTION IF EXISTS dq_percentile(VARCHAR, VARCHAR, VARCHAR, VARCHAR);
CREATE OR REPLACE FUNCTION dq_percentile(pDatabase VARCHAR, pSchemata VARCHAR, pTable VARCHAR, pColumn VARCHAR)
RETURNS TABLE(pColumn_value numeric, percentile_rank double precision) AS $$
BEGIN
  RETURN QUERY EXECUTE format('
    SELECT %I::numeric, PERCENT_RANK() OVER (ORDER BY %I) AS percentile_rank 
    FROM %I.%I.%I', pColumn, pColumn, pDatabase, pSchemata, pTable);
END;
$$ LANGUAGE plpgsql;

-- dq_percentile_top
CREATE TABLE IF NOT EXISTS dq_t_percentile_top (
  id SERIAL PRIMARY KEY,
  database_name VARCHAR(255) NOT NULL,
  schemata_name VARCHAR(255) NOT NULL,
  table_name VARCHAR(255) NOT NULL,
  column_name VARCHAR(255) NOT NULL,
  percentile_90 DECIMAL(14, 4) NULL,
  percentile_95 DECIMAL(14, 4) NULL,
  percentile_99 DECIMAL(14, 4) NULL,
  maximum DECIMAL(14, 4) NULL,
  time TIMESTAMP DEFAULT NOW()
);

DROP FUNCTION IF EXISTS dq_percentile_top_entry(VARCHAR, VARCHAR, VARCHAR, VARCHAR);
CREATE OR REPLACE FUNCTION dq_percentile_top_entry(pDatabase VARCHAR, pSchemata VARCHAR, pTable VARCHAR, pColumn VARCHAR)
RETURNS VOID AS $$
BEGIN
  EXECUTE format('
    INSERT INTO dq_t_percentile_top (database_name, schemata_name, table_name, column_name, percentile_90, percentile_95, percentile_99, maximum)
    WITH percent AS (
      SELECT %I, PERCENT_RANK() OVER (ORDER BY %I) AS percentile_rank
      FROM %I.%I.%I
    )
    SELECT %L, %L, %L, %L,
    (SELECT MAX(%I) FROM percent WHERE percentile_rank <= 0.9) AS "percentile_90",
    (SELECT MAX(%I) FROM percent WHERE percentile_rank <= 0.95) AS "percentile_95",
    (SELECT MAX(%I) FROM percent WHERE percentile_rank <= 0.99) AS "percentile_99",
    MAX(%I) AS "max"
    FROM percent', pColumn, pColumn, pDatabase, pSchemata, pTable,
    pDatabase, pSchemata, pTable, pColumn, pColumn, pColumn, pColumn, pColumn);
END;
$$ LANGUAGE plpgsql;

DROP FUNCTION IF EXISTS dq_percentile_top(VARCHAR, VARCHAR, VARCHAR, VARCHAR);
CREATE OR REPLACE FUNCTION dq_percentile_top(pDatabase VARCHAR, pSchemata VARCHAR, pTable VARCHAR, pColumn VARCHAR)
RETURNS TABLE(percentile_90 DECIMAL, percentile_95 DECIMAL, percentile_99 DECIMAL, max DECIMAL) AS $$
BEGIN
  RETURN QUERY EXECUTE format('
    WITH percent AS (
      SELECT %I, PERCENT_RANK() OVER (ORDER BY %I) AS percentile_rank
      FROM %I.%I.%I
    )
    SELECT
    (SELECT MAX(%I) FROM percent WHERE percentile_rank <= 0.9)::decimal AS "percentile_90",
    (SELECT MAX(%I) FROM percent WHERE percentile_rank <= 0.95)::decimal AS "percentile_95",
    (SELECT MAX(%I) FROM percent WHERE percentile_rank <= 0.99)::decimal AS "percentile_99",
    MAX(%I)::decimal AS "max"
    FROM percent', pColumn, pColumn, pDatabase, pSchemata, pTable, pColumn, pColumn, pColumn, pColumn);
END;
$$ LANGUAGE plpgsql;

-- dq_anomaly
DROP FUNCTION IF EXISTS dq_anomaly_json(VARCHAR, VARCHAR, VARCHAR, VARCHAR);
CREATE OR REPLACE FUNCTION dq_anomaly_json(pDatabase VARCHAR, pSchemata VARCHAR, pTable VARCHAR, pColumn VARCHAR)
RETURNS TABLE(record JSON) AS $$
BEGIN
  RETURN QUERY EXECUTE format('
    WITH statistic AS (
      SELECT AVG(%I) AS mittelwert, STDDEV(%I) AS standardabweichung FROM %I
    )
    SELECT row_to_json(t) 
    FROM %I.%I.%I t, statistic s 
    WHERE ABS(t.%I - s.mittelwert) >= 2 * s.standardabweichung', pColumn, pColumn, pTable, pDatabase, pSchemata, pTable, pColumn);
END;
$$ LANGUAGE plpgsql;

DROP FUNCTION IF EXISTS dq_anomaly(VARCHAR, VARCHAR, VARCHAR, VARCHAR);
CREATE OR REPLACE FUNCTION dq_anomaly(pDatabase VARCHAR, pSchemata VARCHAR, pTable VARCHAR, pColumn VARCHAR)
RETURNS TABLE(anomaly numeric) AS $$
BEGIN
  RETURN QUERY EXECUTE format('
    WITH statistic AS (
      SELECT AVG(%I) AS mittelwert, STDDEV(%I) AS standardabweichung FROM %I.%I.%I
    ),
	anomalies as(
    SELECT * 
    FROM %I.%I.%I t, statistic s 
    WHERE ABS(t.%I - s.mittelwert) >= 2 * s.standardabweichung
	)
	select (select count(*)::numeric from anomalies)/(select count(%I)::numeric from %I.%I.%I);', 
	pColumn, pColumn, pDatabase, pSchemata, pTable, pDatabase, pSchemata, pTable, pColumn, pColumn, pDatabase, pSchemata, pTable);
END;
$$ LANGUAGE plpgsql;


-- dq_histogram
DROP FUNCTION IF EXISTS dq_histogram(VARCHAR, VARCHAR, VARCHAR, VARCHAR);
CREATE OR REPLACE FUNCTION dq_histogram(pDatabase VARCHAR, pSchemata VARCHAR,pTable VARCHAR, pColumn VARCHAR)
RETURNS TABLE(pColumn_value DECIMAL, frequency INTEGER) AS $$
BEGIN
  RETURN QUERY EXECUTE format('
    WITH Histogram AS (
      SELECT %I::decimal, COUNT(*)::integer as Frequency
      FROM %I.%I.%I 
      GROUP BY %I
    )
    SELECT * FROM Histogram 
    ORDER BY %I ASC', pColumn, pDatabase, pSchemata, pTable, pColumn, pColumn);
END;
$$ LANGUAGE plpgsql;

-- dq_missing_values
DROP FUNCTION IF EXISTS dq_missing_values(VARCHAR, VARCHAR,VARCHAR, VARCHAR);
CREATE OR REPLACE FUNCTION dq_missing_values(pDatabase VARCHAR, pSchemata VARCHAR, pTable VARCHAR, pColumn VARCHAR)
RETURNS INTEGER AS $$
DECLARE
  missing_values INTEGER;
BEGIN
  EXECUTE format('SELECT COUNT(*) - COUNT(%I) AS missing_values FROM %I.%I.%I', pColumn, pDatabase, pSchemata, pTable)
  INTO missing_values;
  RETURN missing_values;
END;
$$ LANGUAGE plpgsql;


-- Kolmogorov-Smirnov Statistik

DROP FUNCTION IF EXISTS dq_ks(VARCHAR, VARCHAR,VARCHAR, VARCHAR, VARCHAR, VARCHAR,VARCHAR, VARCHAR);
CREATE OR REPLACE FUNCTION dq_ks
(pBaselineDatabase VARCHAR, pBaselineSchemata VARCHAR, pBaselineTable VARCHAR, pBaselineColumn VARCHAR, 
pComparisonDatabase VARCHAR, pComparisonSchemata VARCHAR, pComparisonTable VARCHAR, pComparisonColumn VARCHAR)
RETURNS NUMERIC AS $$
DECLARE
  ks NUMERIC;
BEGIN
  EXECUTE format('
WITH cdf1 AS (
  SELECT %I,
         COUNT(*) OVER (ORDER BY %I) * 1.0 / (SELECT COUNT(*) FROM %I.%I.%I) AS cdf
  FROM %I.%I.%I
),
cdf2 AS (
  SELECT %I,
         COUNT(*) OVER (ORDER BY %I) * 1.0 / (SELECT COUNT(*) FROM %I.%I.%I) AS cdf
  FROM %I.%I.%I
),
combined AS (
  SELECT cdf1.%I, cdf1.cdf AS cdf1, COALESCE(cdf2.cdf, 0) AS cdf2
  FROM cdf1
  LEFT JOIN cdf2 ON cdf1.%I = cdf2.%I
  UNION ALL
  SELECT cdf2.%I, COALESCE(cdf1.cdf, 0), cdf2.cdf
  FROM cdf2
  LEFT JOIN cdf1 ON cdf1.%I = cdf2.%I
)
SELECT MAX(ABS(cdf1 - cdf2)) AS ks_statistic
FROM combined', pBaselineColumn, pBaselineColumn, 
pBaselineDatabase, pBaselineSchemata, pBaselineTable, pBaselineDatabase, pBaselineSchemata, pBaselineTable,
pComparisonColumn, pComparisonColumn, 
pComparisonDatabase, pComparisonSchemata, pComparisonTable, pComparisonDatabase, pComparisonSchemata, pComparisonTable,
pBaselineColumn,pBaselineColumn,pComparisonColumn,pBaselineColumn,pComparisonColumn,pBaselineColumn)
  INTO ks;
  RETURN ks;
END;
$$ LANGUAGE plpgsql;

CREATE TABLE IF NOT EXISTS dq_t_kolmogorov_smirnov (
  id SERIAL PRIMARY KEY,
  baseline_database_name VARCHAR(255) NOT NULL,
  baseline_schemata_name VARCHAR(255) NOT NULL,
  baseline_table_name VARCHAR(255) NOT NULL,
  baseline_column_name VARCHAR(255) NOT NULL,
  comparison_database_name VARCHAR(255) NOT NULL,
  comparison_schemata_name VARCHAR(255) NOT NULL,
  comparison_table_name VARCHAR(255) NOT NULL,
  comparison_column_name VARCHAR(255) NOT NULL,
  ks_statistic DECIMAL(14, 4) NULL,
  time TIMESTAMP DEFAULT NOW()
);

DROP FUNCTION IF EXISTS dq_ks_entry(VARCHAR, VARCHAR,VARCHAR, VARCHAR, VARCHAR, VARCHAR,VARCHAR, VARCHAR);
CREATE OR REPLACE FUNCTION dq_ks_entry
(pBaselineDatabase VARCHAR, pBaselineSchemata VARCHAR, pBaselineTable VARCHAR, pBaselineColumn VARCHAR, 
pComparisonDatabase VARCHAR, pComparisonSchemata VARCHAR, pComparisonTable VARCHAR, pComparisonColumn VARCHAR)
RETURNS VOID AS $$
BEGIN
  EXECUTE format('
INSERT INTO dq_t_kolmogorov_smirnov (baseline_database_name, baseline_schemata_name, baseline_table_name, baseline_column_name, 
comparison_database_name, comparison_schemata_name, comparison_table_name, comparison_column_name, ks_statistic)
WITH cdf1 AS (
  SELECT %I,
         COUNT(*) OVER (ORDER BY %I) * 1.0 / (SELECT COUNT(*) FROM %I.%I.%I) AS cdf
  FROM %I.%I.%I
),
cdf2 AS (
  SELECT %I,
         COUNT(*) OVER (ORDER BY %I) * 1.0 / (SELECT COUNT(*) FROM %I.%I.%I) AS cdf
  FROM %I.%I.%I
),
combined AS (
  SELECT cdf1.%I, cdf1.cdf AS cdf1, COALESCE(cdf2.cdf, 0) AS cdf2
  FROM cdf1
  LEFT JOIN cdf2 ON cdf1.%I = cdf2.%I
  UNION ALL
  SELECT cdf2.%I, COALESCE(cdf1.cdf, 0), cdf2.cdf
  FROM cdf2
  LEFT JOIN cdf1 ON cdf1.%I = cdf2.%I
)
SELECT %L,%L,%L,%L,%L,%L,%L,%L,MAX(ABS(cdf1 - cdf2))
FROM combined', pBaselineColumn, pBaselineColumn, 
pBaselineDatabase, pBaselineSchemata, pBaselineTable, pBaselineDatabase, pBaselineSchemata, pBaselineTable,
pComparisonColumn, pComparisonColumn, 
pComparisonDatabase, pComparisonSchemata, pComparisonTable, pComparisonDatabase, pComparisonSchemata, pComparisonTable,
pBaselineColumn,pBaselineColumn,pComparisonColumn,pBaselineColumn,pComparisonColumn,pBaselineColumn,
pBaselineDatabase, pBaselineSchemata, pBaselineTable, pBaselineColumn, 
pComparisonDatabase, pComparisonSchemata, pComparisonTable, pComparisonColumn);
END;
$$ LANGUAGE plpgsql;



