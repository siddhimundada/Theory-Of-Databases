INSERT INTO cse532.facility(
SELECT H.FacilityID,H.FacilityName,H.Description,
H.Address1,H.Address2,H.City,H.State,H.ZipCode,
H.CountyCode,H.County,DB2GSE.ST_POINT(H.Longitude, H.Latitude, 1)
FROM cse532.facilityoriginal AS H
);

