INSERT INTO {0} 
SELECT block_fips, cdist_id, 999 AS area_pct 
FROM    
(
    SELECT x.block_fips, cdist_id, row_number() OVER (PARTITION BY block_fips ORDER BY ST_DISTANCE(x.geom::geography, y."GEOMETRY"::geography)) AS row_numb 
    FROM 
    (
		SELECT a.block_fips, substr(a.block_fips, 1, 2) AS state_fips, geom 
		FROM 
		(
			SELECT "BLOCK_FIPS" as block_fips, "GEOMETRY" as geom 
			FROM {2} 
			WHERE substr("BLOCK_FIPS", 1, 2) = '{3}'
		) AS a 
		LEFT JOIN 
		(
			SELECT block_fips 
			FROM {0} 
			WHERE substr(block_fips, 1, 2) = '{3}' 
		) AS b 
		on a.block_fips = b.block_fips 
		WHERE b.block_fips IS NULL 
	) AS x 
	LEFT JOIN 
	(
		SELECT "STATEFP" AS state_fips, "GEOID" AS cdist_id, "GEOMETRY" 
		FROM {1} 
		WHERE "STATEFP" = '{3}' 
	) AS y 
	ON x.state_fips = y.state_fips 
	ORDER BY x.block_fips, cdist_id 
) z 
WHERE row_numb = 1; COMMIT;