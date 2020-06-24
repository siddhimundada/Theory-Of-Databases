drop index cse532.facilityidx;
drop index cse532.zipidx;
drop index cse532.INDEX1;
drop index cse532.INDEX2;
drop index cse532.INDEX3;

create index cse532.facilityidx on cse532.facility(geolocation) extend using db2gse.spatial_index(0.85, 2, 5);

create index cse532.zipidx on cse532.uszip(shape) extend using db2gse.spatial_index(0.85, 2, 5);

create index cse532.INDEX1 on cse532.facilitycertification(FacilityID,AttributeValue);

create index cse532.INDEX2 on cse532.facility(FacilityID,ZipCode);

create index cse532.INDEX3 on cse532.uszip(GeoID10);


runstats on table cse532.facility and indexes all;

runstats on table cse532.uszip and indexes all;

runstats on table cse532.facilitycertification and indexes all;