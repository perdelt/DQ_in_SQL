# Testen der Prozeduren

with statistic as 
	(SELECT DISTINCT alterInJahren, NTILE(4) OVER(ORDER BY alterInJahren) 
	as quartile from t_person)
	Select
	min(alterInJahren) as "min",
	(Select max(alterInJahren) from statistic where quartile = 1) as "Q1",
	(Select max(alterInJahren) from statistic where quartile = 2) as "Q2",
	(Select max(alterInJahren) from statistic where quartile = 3) as "Q3",
	max(alterInJahren) as "max"
	from statistic;

call dq_quartile('t_person', 'alterInJahren');
call dq_quartile_entry('stud_wilke','t_person', 'alterInJahren');
call dq_quartile_entry('sose24_dbs_oltp','person', 'alterInJahren');

###

SELECT COUNT(lieferant) - COUNT(DISTINCT lieferant) 
as zahl_duplikate FROM t_liefert;

call dq_duplicates('t_liefert','lieferant');

####

SELECT
	COUNT(alterInJahren) AS anzahl,
	COUNT(DISTINCT alterInJahren) AS kardinalitaet,
	MIN(alterInJahren) AS minimum,
	MAX(alterInJahren) AS maximum,
	MAX(alterInJahren) - MIN(alterInJahren) AS spannweite,
	AVG(alterInJahren) AS mittelwert,
	STDDEV(alterInJahren) AS standardabweichung
FROM t_person;

call dq_summary('t_person','alterInJahren');
call dq_summary_entry('stud_wilke','t_person', 'alterInJahren');
call dq_summary_entry('sose24_dbs_oltp','person', 'alterInJahren');

###

SELECT
alterInJahren,
PERCENT_RANK() OVER (ORDER BY alterInJahren) AS percentile_rank
FROM t_person;

call dq_percentile('t_person','alterInJahren');

with percent as (
SELECT
alterInJahren,
PERCENT_RANK() OVER (ORDER BY alterInJahren) AS percentile_rank
FROM t_person)
Select
(Select max(alterInJahren) from percent where percentile_rank <=0.9) as '90% percentile',
(Select max(alterInJahren) from percent where percentile_rank <=0.95) as '95% percentile',
(Select max(alterInJahren) from percent where percentile_rank <=0.99) as '99% percentile';


call dq_percentile_top('t_person','alterInJahren');
call dq_percentile_top_entry('stud_wilke','t_person', 'alterInJahren');
call dq_percentile_top_entry('sose24_dbs_oltp','person', 'alterInJahren');

###

WITH statistic AS (
	SELECT
		AVG(alterInJahren) AS mittelwert,
		STDDEV(alterInJahren) AS standardabweichung
	FROM t_person
)
SELECT t.*
FROM t_person t, statistic s
WHERE ABS(t.alterInJahren - s.mittelwert) >= 3 * s.standardabweichung;


call dq_anomaly('t_person','alterInJahren');


###

WITH Histogram(alterInJahren, Frequency) AS
(SELECT alterInJahren, count(*) FROM t_person GROUP BY alterInJahren)
SELECT * FROM Histogram order by alterInJahren asc;

call dq_histogram('t_person','alterInJahren');

###

Select COUNT(*) - COUNT(strasse) as missing_values from t_person; 

call dq_missing_values('t_person','strasse'); 

### PSI

-- Compute PSI for a given variable_of_interest

WITH BaselineCounts AS (
    SELECT name, COUNT(*) AS baseline_count
    FROM artikelgruppe
    GROUP BY name
),
ComparisonCounts AS (
    SELECT name, COUNT(*) AS comparison_count
    FROM t_artikelgruppe
    GROUP BY name
),
PSIContributions AS (
SELECT
    COALESCE(B.baseline_count, 0) AS baseline_count,
    COALESCE(C.comparison_count, 0) AS comparison_count,
    COALESCE(B.baseline_count, 0) - COALESCE(C.comparison_count, 0) AS psi_contrib
FROM BaselineCounts B
LEFT OUTER JOIN ComparisonCounts C ON B.name = C.name
)
SELECT
    SUM(psi_contrib * LOG(CASE WHEN comparison_count = 0 THEN NULL
        ELSE baseline_count / comparison_count END)) AS psi
FROM PSIContributions;


### KS

WITH cdf1 AS (
    SELECT name,
           COUNT(*) OVER (ORDER BY name) * 1.0 / (SELECT COUNT(*) FROM artikelgruppe) AS cdf
    FROM artikelgruppe
),
cdf2 AS (
    SELECT name,
           COUNT(*) OVER (ORDER BY name) * 1.0 / (SELECT COUNT(*) FROM t_artikelgruppe) AS cdf
    FROM t_artikelgruppe
),
combined AS (
    SELECT cdf1.name, cdf1.cdf AS cdf1, COALESCE(cdf2.cdf, 0) AS cdf2
    FROM cdf1
    LEFT JOIN cdf2 ON cdf1.name = cdf2.name
    UNION ALL
    SELECT cdf2.name, COALESCE(cdf1.cdf, 0), cdf2.cdf
    FROM cdf2
    LEFT JOIN cdf1 ON cdf1.name = cdf2.name
)
SELECT MAX(ABS(cdf1 - cdf2)) AS ks_statistic
FROM combined;

