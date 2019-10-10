DROP TABLE IF EXISTS {0}; COMMIT; 
CREATE TABLE {0} 
(
block_fips varchar(15), 
county_fips varchar(5), 
area numeric, 
geom geometry(multipolygon, 4326) 
); COMMIT; 

INSERT INTO {0} 
SELECT a."BLOCK_FIPS" AS block_fips, b."GEOID" AS county_fips, 
(CASE WHEN ST_WITHIN(a."GEOMETRY", b."GEOMETRY") THEN ST_AREA(a."GEOMETRY"::geography) 
    ELSE NULL 
 END) AS area,  
(CASE WHEN ST_WITHIN(a."GEOMETRY", b."GEOMETRY") THEN a."GEOMETRY" 
    ELSE ST_MULTI(ST_BUFFER(ST_INTERSECTION(a."GEOMETRY", b."GEOMETRY"), 0.0)) 
END) AS geom  
FROM {2} AS a, {1} AS b   
WHERE ST_INTERSECTS(a."GEOMETRY", b."GEOMETRY") and SUBSTR(a."BLOCK_FIPS", 1, 5) IN ({3}); COMMIT; 

UPDATE {0} SET area = ST_AREA(geom::geography) WHERE area IS NULL; COMMIT;  