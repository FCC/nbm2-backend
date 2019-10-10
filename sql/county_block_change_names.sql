DROP TABLE IF EXISTS {0}; COMMIT; 
CREATE TABLE {0} 
(
block_fips varchar(15) primary key, 
county_fips varchar(5), 
area_pct numeric 
); COMMIT; 

INSERT INTO {0} 
SELECT "BLOCK_FIPS" AS block_fips, 
{3} AS county_fips, 
1 AS area_pct 
FROM {1} 
WHERE substr("BLOCK_FIPS", 1, 5) NOT IN ({2}); 
COMMIT; 