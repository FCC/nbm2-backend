INSERT INTO {0} 
SELECT a."BLOCK_FIPS" as block_fips, b."GEOID" AS cplace_id, 
(CASE WHEN ST_WITHIN(a."GEOMETRY", b."GEOMETRY") THEN ST_AREA(a."GEOMETRY"::geography) ELSE NULL END) AS area, 
(CASE WHEN ST_WITHIN(a."GEOMETRY", b."GEOMETRY") THEN a."GEOMETRY" ELSE ST_MULTI(ST_BUFFER(ST_INTERSECTION(a."GEOMETRY", b."GEOMETRY"), 0.0)) END) AS geom 
FROM {1} AS a, {2} AS b 
WHERE ST_INTERSECTS(a."GEOMETRY", b."GEOMETRY") AND SUBSTR(a."BLOCK_FIPS", 1, 2) = '{3}' AND b."STATEFP" = '{3}'; COMMIT;