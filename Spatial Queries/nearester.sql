SELECT CURRENT TIMESTAMP FROM SYSIBM.SYSDUMMY1;

delete from cse532.querytime
where queryID ='Q1';

INSERT INTO cse532.querytime VALUES(
'Q1',  current timestamp, null);



select F.FacilityName,C.AttributeValue,F.Geolocation,db2gse.st_distance(db2gse.st_point(-72.993983,40.824369,1), F.Geolocation,
	'STATUTE MILE') as distance 
from cse532.facility as F,cse532.facilitycertification as C
WHERE F.FacilityID=C.FacilityID
AND
db2gse.st_contains(db2gse.st_buffer(db2gse.st_point(-72.993983,40.824369,1),'0.25'), F.Geolocation) = 1 and 
C.AttributeValue='Emergency Department'
order by distance 
fetch first 1 rows only;

SELECT CURRENT TIMESTAMP FROM SYSIBM.SYSDUMMY1;

UPDATE cse532.querytime
SET endtime = current timestamp
WHERE queryID = 'Q1';
SELECT TIMESTAMPDIFF(1, endtime - starttime)
FROM cse532.querytime;


