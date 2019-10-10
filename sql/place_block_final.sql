INSERT INTO {0} 
SELECT a.block_fips, a.cplace_id, round((a.area/c.area)::numeric, 4) AS area_pct 
FROM 
( 
   SELECT block_fips, cplace_id, area, row_number() OVER (PARTITION BY block_fips ORDER BY area DESC) AS row_numb  
   FROM {1} 
   WHERE area >= 1 
) AS a 
LEFT JOIN 
(
   SELECT block_fips, sum(area) AS total_area_intersect 
   FROM {1} 
   GROUP BY block_fips 
) AS b 
ON a.block_fips = b.block_fips 
LEFT JOIN 
(
   SELECT "BLOCK_FIPS" AS block_fips, st_area("GEOMETRY"::geography) AS area 
   FROM {2}  
) AS c 
ON a.block_fips = c.block_fips 
WHERE row_numb = 1 
AND (a.area > (c.area - total_area_intersect)); COMMIT;   
CREATE index ON {0}  (block_fips); COMMIT; 