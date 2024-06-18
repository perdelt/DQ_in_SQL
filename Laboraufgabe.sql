DELIMITER $$ 
SET sql_notes = 0$$ #temporär Warnungen ausschalten

## dq_quartile

CREATE TABLE IF NOT EXISTS `dq_t_quartile` (
  `id` int(11) NOT NULL AUTO_INCREMENT, 
  `database_name` varchar(255) NOT NULL,
  `table_name` varchar(255) NOT NULL,
  `column_name` varchar(255) NOT NULL,
  `minimum` decimal(14,4) NULL,
  `Q1` decimal(14,4) NULL,            
  `Q2` decimal(14,4) NULL,     
  `Q3` decimal(14,4) NULL,    
  `maximum` decimal(14,4) NULL,
  `time` datetime DEFAULT NOW(),
  PRIMARY KEY (`id`)
)$$

DROP PROCEDURE IF EXISTS `dq_quartile_entry`$$
CREATE PROCEDURE `dq_quartile_entry`(IN pDatabase varchar(255), IN pTable varchar(255), IN pColumn varchar(255))
BEGIN
	Set @query = Concat('
	INSERT INTO dq_t_quartile
	(database_name, table_name, column_name, minimum, Q1, Q2, Q3, maximum)
	with statistic as (SELECT DISTINCT ',pColumn,', 
	NTILE(4) OVER(ORDER BY ',pColumn,') as quartile from ',pDatabase,'.',pTable,')
	Select
	''',pDatabase,''', ''',pTable,''', ''',pColumn,''',
	min(',pColumn,') as "min",
	(Select max(statistic.',pColumn,')from statistic where quartile = 1) as "Q1",
	(Select max(statistic.',pColumn,')from statistic where quartile = 2) as "Q2",
	(Select max(statistic.',pColumn,')from statistic where quartile = 3) as "Q3",
	max(',pColumn,') as "max"
	from statistic');
	
	PREPARE dynamic_statement FROM @query;
	EXECUTE dynamic_statement;
	DEALLOCATE PREPARE dynamic_statement;
END$$

DROP PROCEDURE IF EXISTS `dq_quartile`$$

CREATE PROCEDURE `dq_quartile`(IN pTable varchar(255), IN pColumn varchar(255))
BEGIN
	Set @query = Concat('with statistic as (SELECT DISTINCT ',pColumn,
	', NTILE(4) OVER(ORDER BY ',pColumn,') as quartile from ',
	pTable,')
	Select
	min(',pColumn,') as "min",
	(Select max(statistic.',pColumn,')from statistic where quartile = 1) as "Q1",
	(Select max(statistic.',pColumn,')from statistic where quartile = 2) as "Q2",
	(Select max(statistic.',pColumn,')from statistic where quartile = 3) as "Q3",
	max(',pColumn,') as "max"
	from statistic');
	
	PREPARE dynamic_statement FROM @query;
	EXECUTE dynamic_statement;
	DEALLOCATE PREPARE dynamic_statement;
END$$

## dq_duplicates

DROP PROCEDURE IF EXISTS `dq_duplicates`$$
CREATE PROCEDURE `dq_duplicates`(IN pTable varchar(255), IN pColumn varchar(255))
BEGIN
	Set @query = Concat('SELECT Count(',pColumn,
	') - Count(Distinct ',pColumn,') as zahl_duplikate from ',
	pTable);
	
	PREPARE dynamic_statement FROM @query;
	EXECUTE dynamic_statement;
	DEALLOCATE PREPARE dynamic_statement;
END$$

## dq_summary

CREATE TABLE IF NOT EXISTS `dq_t_summary` (
  `id` int(11) NOT NULL AUTO_INCREMENT, 
  `database_name` varchar(255) NOT NULL,
  `table_name` varchar(255) NOT NULL,
  `column_name` varchar(255) NOT NULL,
  `count_value` int(11) NULL,
  `cardinality_value` int(11) NULL,            
  `minimum` decimal(14,4) NULL,     
  `maximum` decimal(14,4) NULL,    
  `span` decimal(14,4) NULL,
  `average` decimal(14,4) NULL,
  `standard_deviation` decimal(14,4) NULL,
  `time` datetime DEFAULT NOW(),
  PRIMARY KEY (`id`)
)$$

DROP PROCEDURE IF EXISTS `dq_summary_entry`$$
CREATE PROCEDURE `dq_summary_entry`(IN pDatabase VARCHAR(255), IN pTable VARCHAR(255), IN pColumn VARCHAR(255))
BEGIN

    Set @query = CONCAT(
    'INSERT INTO dq_t_summary 
		(database_name, table_name, column_name, count_value, cardinality_value, minimum, maximum, 
		span, average, standard_deviation)
		SELECT 
   		''',pDatabase,''', ''',pTable,''', ''',pColumn,''', 
		COUNT(',pColumn,'),
    	COUNT(DISTINCT ',pColumn,'),
    	MIN(',pColumn,'),
    	MAX(',pColumn,'),
    	MAX(',pColumn,') - MIN(',pColumn,'),
    	AVG(',pColumn,'),
    	STDDEV(',pColumn,')
		FROM ',pDatabase,'.',pTable
   	);

    PREPARE dynamic_statement FROM @query;
    EXECUTE dynamic_statement;
    DEALLOCATE PREPARE dynamic_statement;
END$$

DROP PROCEDURE IF EXISTS `dq_summary`$$
CREATE PROCEDURE `dq_summary`(IN pTable varchar(255), IN pColumn varchar(255))
BEGIN
	Set @query = Concat('SELECT COUNT(',pColumn,') AS anzahl,
	COUNT(DISTINCT ',pColumn,') AS cardinality,
	MIN(',pColumn,') AS minimum,
	MAX(',pColumn,') AS maximum,
	MAX(',pColumn,') - MIN(',pColumn,') AS span,
	AVG(',pColumn,') AS average,
	STDDEV(',pColumn,') AS standard_deviation
	FROM ',pTable);
	
	PREPARE dynamic_statement FROM @query;
	EXECUTE dynamic_statement;
	DEALLOCATE PREPARE dynamic_statement;
END$$

## dq_percentile

DROP PROCEDURE IF EXISTS `dq_percentile`$$
CREATE PROCEDURE `dq_percentile`(IN pTable varchar(255), IN pColumn varchar(255))
BEGIN
	Set @query = Concat('SELECT ',pColumn,
	',PERCENT_RANK() OVER (ORDER BY ',pColumn,') as percentile_rank from ',
	pTable);
	
	PREPARE dynamic_statement FROM @query;
	EXECUTE dynamic_statement;
	DEALLOCATE PREPARE dynamic_statement;
END$$

## dq_percentile_top

CREATE TABLE IF NOT EXISTS `dq_t_percentile_top` (
  `id` int(11) NOT NULL AUTO_INCREMENT, 
  `database_name` varchar(255) NOT NULL,
  `table_name` varchar(255) NOT NULL,
  `column_name` varchar(255) NOT NULL,
  `90% percentile` decimal(14,4) NULL,            
  `95% percentile` decimal(14,4) NULL,     
  `99% percentile` decimal(14,4) NULL,    
  `maximum` decimal(14,4) NULL,
  `time` datetime DEFAULT NOW(),
  PRIMARY KEY (`id`)
)$$

DROP PROCEDURE IF EXISTS `dq_percentile_top_entry`$$

CREATE PROCEDURE `dq_percentile_top_entry`(IN pDatabase varchar(255), IN pTable varchar(255), IN pColumn varchar(255))
BEGIN
	Set @query = Concat('
		INSERT INTO dq_t_percentile_top
		(database_name, table_name, column_name, 
		`90% percentile`, `95% percentile`, `99% percentile`, maximum)
		with percent as 
		(SELECT 
		',pColumn,',PERCENT_RANK() OVER (ORDER BY ',pColumn,') AS percentile_rank
		FROM ',pDatabase,'.',pTable,')
		Select
		''',pDatabase,''', ''',pTable,''', ''',pColumn,''', 
		(Select max(',pColumn,')from percent where percentile_rank <=0.9) as "90% percentile",
		(Select max(',pColumn,')from percent where percentile_rank <=0.95) as "95% percentile",
		(Select max(',pColumn,')from percent where percentile_rank <=0.99) as "99% percentile",
		max(',pColumn,') as "max" from ',pDatabase,'.',pTable);
	
	PREPARE dynamic_statement FROM @query;
	EXECUTE dynamic_statement;
	DEALLOCATE PREPARE dynamic_statement;
END$$

DROP PROCEDURE IF EXISTS `dq_percentile_top`$$

CREATE PROCEDURE `dq_percentile_top`(IN pTable varchar(255), IN pColumn varchar(255))
BEGIN
	Set @query = Concat('with percent as (SELECT ',pColumn,
	',PERCENT_RANK() OVER (ORDER BY ',pColumn,') AS percentile_rank
FROM ',pTable,')
Select
(Select max(',pColumn,')from percent where percentile_rank <=0.9) as "90% percentile",
(Select max(',pColumn,')from percent where percentile_rank <=0.95) as "95% percentile",
(Select max(',pColumn,')from percent where percentile_rank <=0.99) as "99% percentile",
max(',pColumn,') as "max" from ',pTable);
	
	PREPARE dynamic_statement FROM @query;
	EXECUTE dynamic_statement;
	DEALLOCATE PREPARE dynamic_statement;
END$$

## dq_anomaly

DROP PROCEDURE IF EXISTS `dq_anomaly`$$
CREATE PROCEDURE `dq_anomaly`(IN pTable varchar(255), IN pColumn varchar(255))
BEGIN
	Set @query = Concat('WITH statistic AS (SELECT AVG(',pColumn,
	') AS mittelwert, STDDEV(',pColumn,') as standardabweichung from ',
	pTable,')
	SELECT t.* 
	FROM ',pTable,' t, statistic s 
	WHERE ABS(t.',pColumn,' - s.mittelwert) >= 3 * s.standardabweichung');
	
	PREPARE dynamic_statement FROM @query;
	EXECUTE dynamic_statement;
	DEALLOCATE PREPARE dynamic_statement;
END$$

## histogramm

DROP PROCEDURE IF EXISTS `dq_histogram`$$
CREATE PROCEDURE `dq_histogram`(IN pTable varchar(255), IN pColumn varchar(255))
BEGIN
	Set @query = Concat('WITH Histogram(',pColumn,', Frequency) AS
	(SELECT ',pColumn,', count(*) FROM ',pTable,' GROUP BY ',pColumn,')
	SELECT * FROM Histogram order by ',pColumn,' asc');
	
	PREPARE dynamic_statement FROM @query;
	EXECUTE dynamic_statement;
	DEALLOCATE PREPARE dynamic_statement;
END$$

## missing values

DROP PROCEDURE IF EXISTS `dq_missing_values`$$
CREATE PROCEDURE `dq_missing_values`(IN pTable varchar(255), IN pColumn varchar(255))
BEGIN
	Set @query = Concat('Select COUNT(*) - COUNT(',pColumn,') as missing_values from ',pTable);
	
	PREPARE dynamic_statement FROM @query;
	EXECUTE dynamic_statement;
	DEALLOCATE PREPARE dynamic_statement;
END$$

SET sql_notes = 1$$









