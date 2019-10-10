DROP TABLE IF EXISTS {0}; COMMIT; 

CREATE TABLE {0}
(
year            numeric,
geoid           character varying(15),
type            character varying,
name            character varying,
centroid_lng    numeric,
centroid_lat    numeric,
bbox_arr        numeric(10,7)[]
); COMMIT; 

INSERT INTO {0} 
SELECT  {7} as year, "GEOID" as geoid, 'state' as type, "NAME" as name, 
        cast("INTPTLON" as numeric) as centroid_lng, 
        cast("INTPTLAT" as numeric) as centroid_lat,
        array [
            st_xmin(st_extent("GEOMETRY")),
            st_ymin(st_extent("GEOMETRY")),
            st_xmax(st_extent("GEOMETRY")),
            st_ymax(st_extent("GEOMETRY"))] as bbox_arr 
FROM {1} 
GROUP BY geoid, name, centroid_lat, centroid_lng; COMMIT; 

INSERT INTO {0} 
SELECT  {7} as year, a."GEOID" as geoid, 'place' as type, 
        concat(a."NAME", ', ', b."STUSPS") as name, 
        cast(a."INTPTLON" as float) as centroid_lng, 
        cast(a."INTPTLAT" as float) as centroid_lat, 
        array [
            st_xmin(st_extent(a."GEOMETRY")),
            st_ymin(st_extent(a."GEOMETRY")),
            st_xmax(st_extent(a."GEOMETRY")),
            st_ymax(st_extent(a."GEOMETRY"))] as bbox_arr 
FROM {2} AS a LEFT JOIN {1} AS b ON a."STATEFP" = b."STATEFP" 
GROUP BY geoid, name, centroid_lat, centroid_lng; COMMIT; 
 
INSERT INTO {0}  
SELECT  {7} as year, a."GEOID" as geoid, 'cd' as type,  
        concat(b."NAME", ' ', a."NAMELSAD") as name, 
        cast(a."INTPTLON" as float) as centroid_lng, 
        cast(a."INTPTLAT" as float) as centroid_lat, 
        array [
            st_xmin(st_extent(a."GEOMETRY")),
            st_ymin(st_extent(a."GEOMETRY")),
            st_xmax(st_extent(a."GEOMETRY")),
            st_ymax(st_extent(a."GEOMETRY"))] as bbox_arr 
FROM {3} AS a LEFT JOIN {1} AS b ON a."STATEFP" = b."STATEFP" 
GROUP BY geoid, name, centroid_lat, centroid_lng; COMMIT; 

INSERT INTO {0} 
SELECT  {7} as year, a."GEOID" AS geoid, 'county' AS type,  
        concat(a."NAME", ', ', b."STUSPS") AS name, 
        CAST(a."INTPTLON" AS float) AS centroid_lng, 
        CAST(a."INTPTLAT" AS float) AS centroid_lat, 
        array [
            st_xmin(st_extent(a."GEOMETRY")), 
            st_ymin(st_extent(a."GEOMETRY")), 
            st_xmax(st_extent(a."GEOMETRY")), 
            st_ymax(st_extent(a."GEOMETRY"))] AS bbox_arr 
FROM {4} AS a LEFT JOIN {1} AS b ON SUBSTR(a."GEOID",1,2) = b."STATEFP" 
GROUP BY geoid, name, centroid_lat, centroid_lng; COMMIT; 
 
INSERT INTO {0} 
SELECT  {7} as year, "GEOID" AS geoid, 'cbsa' AS type, "NAME" AS name, 
        CAST("INTPTLON" AS float) AS centroid_lng, 
        CAST("INTPTLAT" AS float) AS centroid_lat, 
        array [
            st_xmin(st_extent("GEOMETRY")),
            st_ymin(st_extent("GEOMETRY")),
            st_xmax(st_extent("GEOMETRY")),
            st_ymax(st_extent("GEOMETRY"))] AS bbox_arr 
FROM {5} 
GROUP BY geoid, name, centroid_lat, centroid_lng; COMMIT; 
 
INSERT INTO {0} 
SELECT  {7} as year, "GEOID" AS geoid, 'tribal' AS type, "NAMELSAD" AS name,
        CAST("INTPTLON" AS float) AS centroid_lng,
        CAST("INTPTLAT" AS float) AS centroid_lat,
        array [
            st_xmin(st_extent("GEOMETRY")),
            st_ymin(st_extent("GEOMETRY")),
            st_xmax(st_extent("GEOMETRY")),
            st_ymax(st_extent("GEOMETRY"))] AS bbox_arr 
FROM {6}  
GROUP BY geoid, name, centroid_lat, centroid_lng; COMMIT; 