-- Microbenchmark queries for Jackpine
-- Topological relations, all pair joins
--Q1 Find the polygons that are spatially equal to other polygons in arealm merge table
select count(*) from  arealm_merge a1 , arealm_merge a2 where ST_equals(a1.geom, a2.geom);

--Q2 Find the points that are spatially equal to other points in pointlm merge table
select count(*) from  pointlm_merge p1, pointlm_merge p2 where ST_Equals(p1.geom, p2.geom);

--Q3 Find the polygons that are spatially disjoint from other polygons in arealm merge table
select count(*) from  arealm_merge a1, arealm_merge a2 where ST_Disjoint(a1.geom, a2.geom);

--Q4 Find the lines in edges merge table that intersect polygons in arealm merge table
select count(*) from  arealm_merge a, edges_merge e where ST_Intersects(e.geom, a.geom);

--Q5 Find the points in point merge table that intersect polygons in arealm merge table
select count(*) from  arealm_merge a, pointlm_merge p where ST_Intersects(p.geom, a.geom);

--Q6 Find the points in point merge table that intersect lines in edges merge table
select count(*) from  edges_merge e, pointlm_merge p where ST_Intersects(p.geom, e.geom);

--Q7 Find the polygons that touch polygons in arealm merge table
select count(*) from  arealm_merge a1 , arealm_merge a2 where ST_touches(a1.geom, a2.geom);

--Q8 Find the lines in edges merge table that touch polygons in arealm merge table
select count(*) from  arealm_merge a, edges_merge e where ST_Touches(e.geom, a.geom);

--Q9 Find first 5 lines that crosses other lines in edges merge table
select e1.gid  from  edges_merge e1 , edges_merge e2 where ST_crosses(e1.geom, e2.geom) limit 5;

--Q10 Find the lines in edges merge table that cross polygons in arealm merge table
select count(*) from  arealm_merge a, edges_merge e where ST_crosses(e.geom, a.geom);

--Q11 Find the polygons that overlap other polygons in arealm merge table
select count(*) from  arealm_merge a1 , arealm_merge a2 where ST_overlaps(a1.geom, a2.geom);

--Q12 Find the polygons that are within other polygons in arealm merge table
select count(*) from  arealm_merge a1 , arealm_merge a2 where ST_within(a1.geom, a2.geom);

--Q13 Find the lines in edges merge table that are inside the polygons in arealm merge table
select count(*) from  arealm_merge a, edges_merge e where ST_within(e.geom, a.geom);

--Q14 Find the points in pointlm merge table that are inside the polygons in arealm merge table
select count(*) from  arealm_merge a, pointlm_merge p where ST_within(p.geom, a.geom);

--Q15 Find the polygons that contain other polygons in arealm merge table
select count(*) from  arealm_merge a1 , arealm_merge a2 where ST_contains(a1.geom, a2.geom);

-- Topological relations, given object
--Q1 Given the longest line in edges merge table, find all polygons in areawater merge table intersected by it
select count(*) from  areawater_merge aw, edges_merge e where ST_Intersects(e.geom, aw.geom) and e.gid=(select gid as id from edges_merge order by ST_Length(geom) desc limit 1);

--Q2 Given the largest polygon in arealm merge table, find all lines in edges merge table that intersect it
select count(*) from  arealm_merge a, edges_merge e where ST_Intersects(e.geom, a.geom) and a.gid=(select gid from arealm_merge order by ST_Area(geom) desc limit 1);

--Q3 Given the largest polygon in arealm merge table, find all polygons in areawater merge table that overlap it
select count(*) from  arealm_merge al, areawater_merge aw where ST_Overlaps(al.geom, aw.geom) and al.gid=(select gid from arealm_merge order by ST_Area(geom) desc limit 1);

--Q4 Contains Largest polygon contains Given the largest polygon in the areawater merge table, find all points in pointlm merge table contained by it
select count(*) from  pointlm_merge pl, areawater_merge aw where ST_Contains(aw.geom, pl.geom) and aw.gid=(select gid from areawater_merge order by ST_Area(geom) desc limit 1);

--Spatial analysis
--Q1 Find all polygons in arealm merge table that are within 1000 distance units from a given point.
select count(*) from arealm_merge  where ST_Distance (geom, ST_SetSRID(ST_MakePoint(-97.7,30.30), 4326)) < 1000;

--Q2 Find all lines in edges merge table that are inside the bounding box of a given specification.
select count(*) from edges_merge where Within( the_geom,  ST_SetSRID(PolyFromText('POLYGON((-97.7 30.30, -92.7 30.30, -92.7 27.30, -97.7 27.30, -97.7 30.30))') , 4326));

--Q3 Find the dimension of all polygons in arealm merge table
select ST_Dimension(a.geom) from  arealm_merge a;

--Q4 Find the envelopes of the first 1000 lines in edges merge table
select st_astext(ST_Envelope(e.geom)) from  edges_merge e limit 1000;

--Q5 Find the longest line in edges merge table
SELECT  gid, ST_Length(geom) AS line FROM edges_merge order BY line DESC limit 1;

--Q6 Find the largest polygon in areawater merge table
SELECT  gid, ST_Area(geom) AS area FROM areawater_merge order BY area DESC limit 1;

--Q7 Determine the total length of all lines in edges merge table
SELECT sum(ST_Length(geom))/1000 AS km_roads FROM edges_merge;

--Q8 Determine the total area of all polygons in areawater merge table
SELECT sum(ST_Area(geom)) AS area FROM areawater_merge;

--Q9 Construct the buffer regions around one mile radius of all polygons in arealm merge table
select ST_Buffer(a.geom,5280) from  arealm_merge a;

--Q10 Construct the convex hulls of all polygons in arealm merge table
select ST_ConvexHull(a.geom) from  arealm_merge a;

-- Macro benchmark
--Q1
SELECT name, st, st_distance(geom, ST_GeomFromText('POINT(-97.73628383792807 30.42241116546868)', 4326 )) as dist FROM cityinfo order by dist limit 1;

--Q2
SELECT fullname, lfromadd, ltoadd, rfromadd, rtoadd, zipl, zipr, st_distance(geom, st_GeomFromText('POINT(-97.73628383792807 30.42241116546868)', 4326)) as d FROM edges_merge where St_Intersects(geom, st_GeomFromText('POLYGON ((-97.72628383792807 30.432411165468682, -97.74628383792808 30.432411165468682, -97.74628383792808 30.41241116546868, -97.72628383792807 30.41241116546868, -97.72628383792807 30.432411165468682))', 4326)) and st_distance(geom, st_GeomFromText('POINT(-97.73628383792807 30.42241116546868)', 4326)) < 0.1 and roadflg = 'Y' order by d limit 1;

--Q3
select gid, feature_na, st_astext(geom) as location from gnis_old  where state_alph='TX' and feature_na='Concordia University at San Antonio' and feature_cl='School' limit 1;

--Q4
SELECT gid,encode(st_asBinary(st_force2d(geom),'XDR'),'base64') as the_geom FROM gnis_old WHERE geom && st_GeomFromText('POLYGON ((-97.30505976311633 32.83432236788476, -97.60109203688367 32.83432236788476, -97.60109203688367 32.80204523211524, -97.30505976311633 32.80204523211524, -97.30505976311633 32.83432236788476))', 4326);

--Q5
SELECT gid,encode(st_asBinary(st_force2d(geom),'XDR'),'base64') as the_geom FROM arealm_merge WHERE geom && st_GeomFromText('POLYGON ((-97.30505976311633 32.83432236788476, -97.60109203688367 32.83432236788476, -97.60109203688367 32.80204523211524, -97.30505976311633 32.80204523211524, -97.30505976311633 32.83432236788476))', 4326);

--Q6
SELECT gid,encode(st_asBinary(st_force2d(geom),'XDR'),'base64') as the_geom FROM areawater_merge WHERE geom && st_GeomFromText('POLYGON ((-97.30505976311633 32.83432236788476, -97.60109203688367 32.83432236788476, -97.60109203688367 32.80204523211524, -97.30505976311633 32.80204523211524, -97.30505976311633 32.83432236788476))', 4326);

--Q7
SELECT gid,encode(st_asBinary(st_force2d(geom),'XDR'),'base64') as the_geom FROM pointlm_merge WHERE geom && st_GeomFromText('POLYGON ((-97.30505976311633 32.83432236788476, -97.60109203688367 32.83432236788476, -97.60109203688367 32.80204523211524, -97.30505976311633 32.80204523211524, -97.30505976311633 32.83432236788476))', 4326);

--Q8
SELECT gid,encode(st_asBinary(st_force2d(geom),'XDR'),'base64') as the_geom FROM edges_merge WHERE geom && st_GeomFromText('POLYGON ((-97.30505976311633 32.83432236788476, -97.60109203688367 32.83432236788476, -97.60109203688367 32.80204523211524, -97.30505976311633 32.80204523211524, -97.30505976311633 32.83432236788476))', 4326);

--Q9
select distinct fld_ar_id from s_gen_struct st, s_fld_haz_ar fa where st.struct_typ='Dam' and ST_Intersects(st.geom, fa.geom);

--Q10
select fld_zone, sum(st_area(s_fld_haz_ar.geom)/43560) as area from s_fld_haz_ar group by fld_zone;

--Q11
select lu.property_i from landuse as lu, s_fld_haz_ar fz where lu.general_la in (100, 113, 150, 160, 200) and (fld_zone = 'A' or fld_zone = 'AE' or fld_zone = 'AO' or fld_zone = 'V') and st_overlaps(lu.geom, fz.geom) limit 10;

--Q12
select lu.property_i from landuse as lu, s_fld_haz_ar fz where lu.general_la = 500 and (fld_zone = 'A' or fld_zone = 'AE' or fld_zone = 'AO' or fld_zone = 'V') and st_overlaps(lu.geom, fz.geom) limit 10;

--Q13
select sum(pa.mkt_value)/sum(st_area(pa.geom)) from parcels192 as pa where pa.stat_land_ like 'A%';

--Q14
select count(*) from  landuse as lu, hospital2 as h where lu.general_la in (100,113,150,160,200) and  st_dwithin(lu.geom, h.geom, 5280);

--Q15
select sc.name, avg(pa.marketvalu) as avg_property_value from parcels2008 as pa, hospitals as sc where st_dwithin(pa.the_geom, sc.the_geom, 5280) group by sc.name order by avg_property_value desc;

--Q16
select property_i from  landuse as lu, frontyard_parking_restrictions fypr where lu.general_la=400  and  ST_Overlaps(fypr.geom, lu.geom);

--Q17
select lu.property_i from  landuse as lu, landfills lf where lu.general_la=300 and lf.permitted='UNPERMITTED' and ST_Intersects(lf.geom,lu.geom);

--Q18
select lake.wtr_nm, geo_id, pa.mkt_value as property_value from parcels192 as pa, s_wtr_ar as lake where st_dwithin(lake.geom, pa.geom, 100) and pa.mkt_value > 0 limit 10;


-- Q2
select count(*) from edges_merge where st_within( geom,  ST_SetSRID(st_polyfromtext('POLYGON((-97.7 30.30, -92.7 30.30, -92.7 27.30, -97.7 27.30, -97.7 30.30))') ,4326));

-- Q3
select ST_Dimension(a.geom) from  arealm_merge a;

-- Q4
select ST_Buffer(a.geom,5280) from  arealm_merge a;

-- Q5
select ST_ConvexHull(a.geom) from  arealm_merge a;

-- Q6
select st_astext(ST_Envelope(e.geom)) from  edges_merge e limit 1000;

-- Q7
SELECT  gid, ST_Length(geom) AS line FROM edges_merge order BY line DESC limit 1;

-- Q8
SELECT  gid, ST_Area(geom) AS area FROM areawater_merge order BY area DESC limit 1;

-- Q9
SELECT sum(ST_Length(geom))/1000 AS km_roads FROM edges_merge;

-- Q10
SELECT sum(ST_Area(geom)) AS area FROM areawater_merge;

--SPATIAL JOIN
-- Q11
select count(*) from  areawater_merge a, edges_merge e where ST_Intersects(e.geom, a.geom) and e.gid=(select gid as id from edges_merge order by ST_Length(geom) desc limit 1);

-- Q12
select count(*) from  arealm_merge a, edges_merge e where ST_Intersects(e.geom, a.geom) and a.gid=(select gid from arealm_merge order by ST_Area(geom) desc limit 1);

-- Q13
select count(*) from  arealm_merge al, areawater_merge aw where ST_Overlaps(al.geom, aw.geom) and al.gid=(select gid from arealm_merge order by ST_Area(geom) desc limit 1);

-- Q14 DEVOLVE 0
select count(*) from  pointlm_merge pl, areawater_merge aw where ST_Contains(aw.geom, pl.geom) and aw.gid=(select gid from areawater_merge order by ST_Area(geom) desc limit 1);

-- ALLPAIR SPATIAL JOIN
-- POLYGON AND POLYGON
-- Q15
select count(*) from  arealm_merge a1 , arealm_merge a2 where ST_overlaps(a1.geom, a2.geom);

-- Q16
select count(*) from  arealm_merge a1 , arealm_merge a2 where ST_contains(a1.geom, a2.geom);

-- Q17
select count(*) from  arealm_merge a1 , arealm_merge a2 where ST_within(a1.geom, a2.geom);

-- Q18
select count(*) from  arealm_merge a1 , arealm_merge a2 where ST_touches(a1.geom, a2.geom);

-- Q19
select count(*) from  arealm_merge a1 , arealm_merge a2 where ST_equals(a1.geom, a2.geom);

-- Q20
select count(*) from  arealm_merge a1, arealm_merge a2 where ST_Disjoint(a1.geom, a2.geom);

-- LINE AND POLYGON
-- Q21
select count(*) from  arealm_merge a, edges_merge e where ST_Intersects(e.geom, a.geom);

-- Q22
select count(*) from  arealm_merge a, edges_merge e where ST_Overlaps(e.geom, a.geom);

-- Q23
select count(*) from  arealm_merge a, edges_merge e where ST_crosses(e.geom, a.geom);

-- Q24
select count(*) from  arealm_merge a, edges_merge e where ST_within(e.geom, a.geom);

-- Q25
select count(*) from  arealm_merge a, edges_merge e where ST_Touches(e.geom, a.geom);

-- LINE AND LINE
-- Q26
select e1.gid  from  edges_merge e1 , edges_merge e2 where ST_overlaps(e1.geom, e2.geom) limit 5;

-- Q27
select e1.gid  from  edges_merge e1 , edges_merge e2 where ST_crosses(e1.geom, e2.geom) limit 5;

-- POINT AND
-- Q28
select count(*) from  arealm_merge a, pointlm_merge p where ST_Within(p.the_geom, a.the_geom);

-- Q29
select count(*) from  arealm_merge a, pointlm_merge p where ST_Intersects(p.the_geom, a.the_geom);

-- Q30
select count(*) from  edges_merge e, pointlm_merge p where ST_Intersects(p.the_geom, e.the_geom);

-- Q31
select count(*) from  pointlm_merge p1, pointlm_merge p2 where ST_Equals(p1.the_geom, p2.the_geom);

-- MACRO BENCHMARK: REVERSE GEOCODING PRECISAM DE CÓDIGO
-- Q32 
SELECT name, st, distance(the_geom, GeomFromText(?, 4326)) as dist FROM cityinfo order by dist limit 1;

-- Q33
SELECT fullname,lfromadd,ltoadd,rfromadd,rtoadd,zipl,zipr, distance(the_geom, GeomFromText(?, 4326 ))  as d FROM edges_merge   where St_Intersects(the_geom, GeomFromText(?,4326))    and distance(the_geom, GeomFromText(?, 4325 )) < 0.1   and  roadflg='Y' order by d limit 1