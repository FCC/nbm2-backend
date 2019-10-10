DROP TABLE IF EXISTS {0}; COMMIT; 
CREATE TABLE {0} 
(
id varchar(13), 
type varchar(6), 
name varchar(74) 
); COMMIT; 

INSERT INTO {0} 
SELECT "GEOID" AS id, 'state' AS type, "NAME" AS name 
FROM {1}; COMMIT; 

INSERT INTO {0} 
SELECT "GEOID" AS id, 'county' AS type, "NAME" AS name 
FROM {2}; COMMIT; 

INSERT INTO {0} 
SELECT "GEOID" AS id, 'cbsa' AS type, "NAME" AS name  
FROM {3}; COMMIT; 

INSERT INTO {0} 
SELECT "GEOID" AS id, 'tribal' AS type, "NAME" AS name  
FROM {4}; COMMIT; 

INSERT INTO {0} 
SELECT "GEOID" AS id, 'CDP' AS type, "NAME" AS name  
FROM {5}; COMMIT; 

INSERT INTO {0} 
SELECT "GEOID" AS id, 'cdist' AS type, "NAMELSAD" AS name 
FROM {6}; COMMIT;