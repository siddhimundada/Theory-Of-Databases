delete from cse532.querytime
where queryID ='Q2';
INSERT INTO cse532.querytime VALUES(
'Q2',  current timestamp, null);

-- SELECT
--     YEAR (current timestamp) current_year,
--     MONTH (current timestamp) current_month,
--     DAY (current timestamp) current_day,
--     HOUR (current timestamp) current_hour,
--     MINUTE (current timestamp) current_minute,
--     SECOND (current timestamp) current_second,
--     MICROSECOND (current timestamp) current_microsecond
-- FROM
--     sysibm.sysdummy1;

with a1 as(
select distinct substring(a.ZipCode,1,5) as Z1 ,e.AttributeValue as av 
from cse532.facility as a ,cse532.facilitycertification as e,cse532.uszip as c
where a.FacilityID=e.FacilityID and substring(a.ZipCode,1,5)=c.GeoID10 and e.AttributeValue in ('Emergency Department')),

a5 as(
select distinct substring(a4.ZipCode,1,5) as z3
from cse532.facility a4,cse532.uszip b
where substring(a4.ZipCode,1,5)=b.GeoID10),

a3 as(
select distinct a2.z1 ,a5.z3 as r1
from a1 as a2 cross join a5,cse532.uszip as c,cse532.uszip as d
WHERE c.GeoID10 = a2.z1 and d.GeoID10 = a5.z3 and db2gse.ST_touches (c.shape, d.shape)=1 )

select z3 from a5 
except 
select distinct z1 from a1 
except 
select distinct r1 from a3 ;

SELECT CURRENT TIMESTAMP FROM SYSIBM.SYSDUMMY1;
UPDATE cse532.querytime
SET endtime = current timestamp
WHERE queryID = 'Q2';
SELECT TIMESTAMPDIFF(1, endtime - starttime)
FROM cse532.querytime;
