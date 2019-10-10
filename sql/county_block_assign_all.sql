INSERT INTO {0} 
SELECT block_fips, county_fips, 999 AS area_pct 
FROM 
(
    SELECT x.block_fips, county_fips, row_number() OVER (PARTITION BY block_fips ORDER BY st_distance(x.geom::geography, y.geom::geography)) AS row_numb 
    FROM 
    (	SELECT a.block_fips, substr(a.block_fips, 1, 2) AS state_fips, "GEOMETRY" AS geom 
        FROM 
        (
            SELECT "BLOCK_FIPS" AS block_fips, "GEOMETRY" 
            FROM {1} 
            WHERE substr("BLOCK_FIPS", 1, 5) IN ({3}) 
        ) AS a 
        LEFT JOIN 
        (
            SELECT block_fips 
            FROM {0} 
            WHERE substr(block_fips, 1, 5) IN ({3}) 
        ) AS b 
        ON a.block_fips = b.block_fips 
        WHERE b.block_fips IS NULL  
    ) AS x 
    LEFT JOIN 
    (
        SELECT SUBSTR("GEOID",1,2) AS state_fips, "GEOID" AS county_fips, "GEOMETRY" As geom
        FROM {2} 
        WHERE "GEOID" IN ({3}) 
    ) AS y 
    ON x.state_fips = y.state_fips 
    ORDER BY x.block_fips, county_fips 
) AS z 
WHERE row_numb = 1; COMMIT;