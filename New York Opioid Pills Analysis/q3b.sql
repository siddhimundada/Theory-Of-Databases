EXPORT TO q3b_result.csv OF DEL MODIFIED BY NOCHARDEL 
WITH A1 AS(
Select MME,BUYER_ZIP as ZIP,ZPOP
from cse532.dea_ny c,CSE532.ZIPPOP z
where z.ZIP = c.BUYER_ZIP),
A2 AS (SELECT ZPOP,SUM(MME) AS M,ZIP
FROM A1
GROUP BY ZIP,ZPOP),

A3 AS(SELECT M,ZPOP ,ZIP,
case WHEN ZPOP <> 0
               then M/ZPOP
            else 0 end AS NORMALISED_MME
FROM A2)
SELECT 
RANK() OVER (ORDER BY NORMALISED_MME DESC) AS RANK,
ZIP,NORMALISED_MME
FROM A3
FETCH FIRST 5 ROWS ONLY;
