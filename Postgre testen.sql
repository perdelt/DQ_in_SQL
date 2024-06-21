-- Temporäre Warnungen ausschalten
SET client_min_messages TO WARNING;

-- Quartile Berechnung
WITH statistic AS (
  SELECT DISTINCT alterInJahren, NTILE(4) OVER (ORDER BY alterInJahren) AS quartile
  FROM t_person
)
SELECT
  MIN(alterInJahren) AS "min",
  (SELECT MAX(alterInJahren) FROM statistic WHERE quartile = 1) AS "Q1",
  (SELECT MAX(alterInJahren) FROM statistic WHERE quartile = 2) AS "Q2",
  (SELECT MAX(alterInJahren) FROM statistic WHERE quartile = 3) AS "Q3",
  MAX(alterInJahren) AS "max"
FROM statistic;

SELECT * from public.dq_quartile('t_person', 'alterinjahren'); 
select dq_quartile_entry('stud_wilke','t_person', 'alterinjahren');

-- Duplicates Check
SELECT COUNT(lieferant) - COUNT(DISTINCT lieferant) AS zahl_duplikate
FROM t_liefert;

select dq_duplicates('t_liefert','lieferant');

-- Summary Statistik
SELECT
  COUNT(alterInJahren) AS anzahl,
  COUNT(DISTINCT alterInJahren) AS kardinalitaet,
  MIN(alterInJahren) AS minimum,
  MAX(alterInJahren) AS maximum,
  MAX(alterInJahren) - MIN(alterInJahren) AS spannweite,
  AVG(alterInJahren) AS mittelwert,
  STDDEV(alterInJahren) AS standardabweichung
FROM t_person;

select * from dq_summary('t_person', 'alterinjahren');
select dq_summary_entry('stud_wilke','t_person', 'alterinjahren');

-- Percentile Berechnung
SELECT
  alterInJahren,
  PERCENT_RANK() OVER (ORDER BY alterInJahren) AS percentile_rank
FROM t_person;

select * from dq_percentile('t_person', 'alterinjahren');

-- Top Percentile Berechnung
WITH percent AS (
  SELECT
    alterInJahren,
    PERCENT_RANK() OVER (ORDER BY alterInJahren) AS percentile_rank
  FROM t_person
)
SELECT
  (SELECT MAX(alterInJahren) FROM percent WHERE percentile_rank <= 0.9) AS "90% percentile",
  (SELECT MAX(alterInJahren) FROM percent WHERE percentile_rank <= 0.95) AS "95% percentile",
  (SELECT MAX(alterInJahren) FROM percent WHERE percentile_rank <= 0.99) AS "99% percentile";
 
select * from dq_percentile_top('t_person', 'alterinjahren');
select dq_percentile_top_entry('stud_wilke','t_person', 'alterinjahren');

-- Anomalie
WITH statistic AS (
  SELECT
    AVG(alterinjahren) AS mittelwert,
    STDDEV(alterinjahren) AS standardabweichung
  FROM t_person
)
SELECT t.*
FROM t_person t, statistic s
WHERE ABS(t.alterinjahren - s.mittelwert) >= 2 * s.standardabweichung;

with anomaly as (select dq_anomaly('t_person', 'alterinjahren'))
select dq_anomaly::json->'alterinjahren' as test from anomaly; 

select (select dq_anomaly('t_person', 'alterinjahren'))::json->'alterinjahren' as "alter";
select dq_anomaly2('t_person', 'alterinjahren');

-- Histogramm
WITH Histogram AS (
  SELECT alterInJahren, COUNT(*) AS Frequency
  FROM t_person
  GROUP BY alterInJahren
)
SELECT *
FROM Histogram
ORDER BY alterInJahren ASC;

select * from dq_histogram('t_person', 'alterinjahren'); 

-- Fehlende Werte
SELECT COUNT(*) - COUNT(strasse) AS missing_values
FROM t_person;

select * from dq_missing_values('t_person', 'strasse');  

-- PSI Berechnung
WITH BaselineCounts AS (
  SELECT "name", COUNT(*) AS baseline_count
  FROM artikelgruppe
  GROUP BY "name"
),
ComparisonCounts AS (
  SELECT "name", COUNT(*) AS comparison_count
  FROM t_artikelgruppe
  GROUP BY "name"
),
PSIContributions AS (
  SELECT
    COALESCE(B.baseline_count, 0) AS baseline_count,
    COALESCE(C.comparison_count, 0) AS comparison_count,
    COALESCE(B.baseline_count, 0) - COALESCE(C.comparison_count, 0) AS psi_contrib
  FROM BaselineCounts B
  FULL OUTER JOIN ComparisonCounts C ON B."name" = C."name"
)
SELECT
  SUM(psi_contrib * LOG(CASE WHEN comparison_count = 0 THEN NULL
    ELSE baseline_count / comparison_count END)) AS psi
FROM PSIContributions;

-- KS Berechnung
WITH cdf1 AS (
  SELECT "name",
         COUNT(*) OVER (ORDER BY "name") * 1.0 / (SELECT COUNT(*) FROM artikelgruppe) AS cdf
  FROM artikelgruppe
),
cdf2 AS (
  SELECT "name",
         COUNT(*) OVER (ORDER BY "name") * 1.0 / (SELECT COUNT(*) FROM t_artikelgruppe) AS cdf
  FROM t_artikelgruppe
),
combined AS (
  SELECT cdf1."name", cdf1.cdf AS cdf1, COALESCE(cdf2.cdf, 0) AS cdf2
  FROM cdf1
  LEFT JOIN cdf2 ON cdf1."name" = cdf2."name"
  UNION ALL
  SELECT cdf2."name", COALESCE(cdf1.cdf, 0), cdf2.cdf
  FROM cdf2
  LEFT JOIN cdf1 ON cdf1."name" = cdf2."name"
)
SELECT MAX(ABS(cdf1 - cdf2)) AS ks_statistic
FROM combined;







-- PSI Berechnung
WITH BaselineCounts AS (
  SELECT "name", COUNT(*) AS baseline_count
  FROM artikelgruppe
  GROUP BY "name"
),
ComparisonCounts AS (
  SELECT "name", COUNT(*) AS comparison_count
  FROM t_artikelgruppe
  GROUP BY "name"
),
PSIContributions AS (
  SELECT
    COALESCE(B.baseline_count, 0) AS baseline_count,
    COALESCE(C.comparison_count, 0) AS comparison_count,
    COALESCE(B.baseline_count, 0) - COALESCE(C.comparison_count, 0) AS psi_contrib
  FROM BaselineCounts B
  FULL OUTER JOIN ComparisonCounts C ON B."name" = C."name"
)
SELECT
  SUM(psi_contrib * LOG(CASE WHEN comparison_count = 0 THEN NULL
    ELSE baseline_count / comparison_count END)) AS psi
FROM PSIContributions;

WITH BaselineCounts AS (
  SELECT "name", COUNT(*) AS baseline_count
  FROM artikelgruppe
  GROUP BY "name"
),
ComparisonCounts AS (
  SELECT "name", COUNT(*) AS comparison_count
  FROM t_artikelgruppe
  GROUP BY "name"
)
  Select
  COALESCE(B.baseline_count, 0) AS baseline_count,
    COALESCE(C.comparison_count, 0) AS comparison_count,
    COALESCE(B.baseline_count, 0) - COALESCE(C.comparison_count, 0) AS psi_contrib
  FROM BaselineCounts B
  full OUTER JOIN ComparisonCounts C ON B."name" = C."name";
 
 select sum(lagerbestand) from artikelgruppe;

select 5*null;
 



