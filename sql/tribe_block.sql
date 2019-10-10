DROP TABLE IF EXISTS {0}; COMMIT; 
CREATE TABLE {0}
(
    block_fips  varchar(15),
    tribal_id   varchar(5),
    aianhhcc    varchar(2),
    area        numeric,
    geom        geometry(multipolygon, {2})
); COMMIT; 
DROP TABLE IF EXISTS {1}; COMMIT; 
CREATE TABLE {1}
(
    block_fips varchar(15),
    tribal_id varchar(5),
    aianhhcc varchar(2),
    area_pct numeric
); commit; 
