INSERT INTO {0} 
SELECT a.block_fips, cdist_id, round((a.area/b.area)::numeric, 4) AS area_pct 
FROM 
(
SELECT block_fips, cdist_id, area, row_number() OVER (PARTITION BY block_fips ORDER BY area DESC) AS row_numb 
FROM {1} 
WHERE area >= 1  
) AS a 
LEFT JOIN 
(
SELECT "BLOCK_FIPS" AS block_fips, ST_AREA("GEOMETRY"::geography) AS area  
FROM {2}   
) AS b 
ON a.block_fips = b.block_fips 
WHERE row_numb = 1; 
COMMIT; 

CREATE index ON {0} (block_fips); COMMIT; 
CREATE index ON {0} (SUBSTR(block_fips, 1, 2)); COMMIT; 

