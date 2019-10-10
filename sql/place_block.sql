DROP TABLE IF EXISTS {0}; COMMIT;
CREATE TABLE {0} 
(
    block_fips  varchar(15),
    cplace_id   varchar(7),
    area        numeric,
    geom        geometry(multipolygon, {2})
); COMMIT; 
DROP TABLE IF EXISTS {1}; COMMIT; 
CREATE TABLE {1}
(
    block_fips varchar(15),
    cplace_id varchar(7),
    area_pct numeric
); COMMIT; 