DROP TABLE IF EXISTS {0}; COMMIT; 
CREATE TABLE {0} 
(
{1} 			character varying(15),
stateabbr 		character varying(2),
hu 				numeric,  
hh 				numeric,  
pop 			numeric,  
h2only_undev	integer,
state_fips 		character varying(2),
urban_rural 	character varying(1),
county_fips 	character varying(5),
cbsa_code 		character(5),
tribal_non 		character(1),
tribal_id 		character varying(5),
aianhhcc 		character(2),
cplace_id 		character(7),
cdist_id 		character(4)
); COMMIT; 

INSERT INTO {0} 
SELECT a.block_fips AS {1}, stateabbr, coalesce(hu{2}, 0) AS hu, coalesce(hh{2}, 0) AS hh, coalesce(pop{2}, 0) AS pop, 
(case when aland{11} = 0 then 1 when (coalesce(hu{2}, 0) = 0 and coalesce(pop{2}, 0) = 0 and aland{11} > 0) then 2 else 0 end) as h2only_undev,
substr(a.block_fips, 1, 2) AS state_fips, urban_rural, d.county_fips, cbsa_code, tribal_non, tribal_id, aianhhcc, cplace_id, cdist_id 
from 
( 
	SELECT "BLOCK_FIPS" AS block_fips, "COUNTY_FIPS" AS county_fips, "ALAND{11}" AS aland{11} 
	FROM {3} 
) AS a 
LEFT JOIN 
(
	SELECT block_fips, hu{2}, hh{2}, pop{2} 
	FROM {4} 
) AS b 
ON a.block_fips = b.block_fips 
LEFT JOIN 
(
	SELECT {1} AS block_fips, stateabbr, urban_rural, tribal_non 
	FROM {5}   
) AS c 
ON a.block_fips = c.block_fips 
LEFT JOIN 
(
	SELECT block_fips, county_fips
	FROM {6} 
) AS d 
ON a.block_fips = d.block_fips 
LEFT JOIN 
(
	SELECT block_fips, cplace_id 
	FROM {7} 
) AS e 
ON a.block_fips = e.block_fips 
LEFT JOIN 
(
	SELECT block_fips, tribal_id, aianhhcc 
	FROM {8} 
) AS f 
ON a.block_fips = f.block_fips 
LEFT JOIN 
(
	SELECT block_fips, cdist_id 
	FROM {9} 
) AS g 
ON a.block_fips = g.block_fips 
LEFT JOIN 
(
	SELECT county_fips, cbsa_code 
	FROM {10}  
) AS h 
ON a.county_fips = h.county_fips; COMMIT; 