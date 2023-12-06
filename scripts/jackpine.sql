-- Topological relations, all pair joins

-- Operation: Equals, Description: Polygon equals Polygon
-- Query: Find the polygons that are spatially equal to other polygons in arealm merge table
select a1.fullname from  arealm_merge a1 , arealm_merge a2 where ST_equals(a1.geom, a2.geom);

-- Operation: Equals, Description: Point equals Point
-- Query: Find the points that are spatially equal to other points in pointlm merge table
select count(*) from  pointlm_merge p1, pointlm_merge p2 where ST_Equals(p1.geom, p2.geom);

-- Operation: Disjoint, Description: Polygon disjoint Polygon
-- Query: Find the polygons that are spatially disjoint from other polygons in arealm_merge table
select count(*) from  arealm_merge a1, arealm_merge a2 where ST_Disjoint(a1.geom, a2.geom)

-- Operation: Intersects, Description: Line intersects Polygon
-- Query: Find the lines in edges merge table that intersect polygons in arealm merge table
select a.fullname, e.fullname from  arealm_merge a, edges_merge e where ST_Intersects(e.geom, a.geom)

-- Operation: Intersects, Description: Point intersects Polygon
-- Query: Find the points in pointlm_merge table that intersect polygons in arealm_merge table
select count(*) from  arealm_merge a, pointlm_merge p where ST_Intersects(p.geom, a.geom)

-- Operation: Intersects, Description: Point intersects Line
-- Query: Find the points in pointlm_merge table that intersect lines in edges_merge table
select count(*) from  edges_merge e, pointlm_merge p where ST_Intersects(p.geom, e.geom)

-- Operation: Touches, Description: Polygon touches Polygon
-- Query: Find the polygons that touch polygons in arealm merge table
select a1.fullname from  arealm_merge a1 , arealm_merge a2 where ST_Touches(a1.geom, a2.geom)

-- Operation: Touches, Description: Line touches Polygon
-- Query: Find the lines in edges_merge table that touch polygons in arealm_merge table
select e.fullname from  edges_merge e , arealm_merge a where ST_Touches(e.geom, a.geom)

-- Operation: Crosses, Description: Line crosses line 
-- Query: Find first 5 lines that crosses other lines in edges_merge table
select e1.fullname  from  edges_merge e1 , edges_merge e2 where ST_crosses(e1.geom, e2.geom) limit 5

-- Operation: Crosses, Description: Line crosses polygon 
-- Query: Find the lines in edges_merge table that cross polygons in arealm_merge table
select e.fullname from  edges_merge e , arealm_merge a where ST_crosses(e.geom, a.geom)

-- Operation: Overlaps, Description: Polygon overlaps polygon 
-- Query: Find the polygons that overlap other polygons in arealm_merge table
select a1.fullname from  arealm_merge a1 , arealm_merge a2 where ST_overlaps(a1.geom, a2.geom)

-- Operation: Within, Description: Polygon within polygon 
-- Query: Find the polygons that are within other polygons in arealm merge table
select a1.fullname from  arealm_merge a1 , arealm_merge a2 where ST_within(a1.geom, a2.geom)

-- Operation: Within, Description: Line within polygon 
-- Query: Find the lines in edges merge table that are inside the polygons in arealm merge table
select e.fullname from  edges_merge e , arealm_merge a where ST_within(e.geom, a.geom)

-- Operation: Within, Description: Point within polygon 
-- Query: Find the points in pointlm merge table that are inside the polygons in arealm merge table
select count(*) from  arealm_merge a, pointlm_merge p where ST_Within(p.geom, a.geom)

-- Operation: Contains, Description: Polygon contains polygon 
-- Query: Find the polygons that contain other polygons in arealm merge table
select a1.fullname from  arealm_merge a1 , arealm_merge a2 where ST_contains(a1.geom, a2.geom)

-- Topological relations, given object
-- Opearation: Intersects, Description: Longest line intersects 
-- Query: Given the longest line in edges merge table, find all polygons in areawater merge table intersected by it
select count(*) from  areawater_merge a, edges_merge e where ST_Intersects(e.geom, a.geom) and e.gid= (select gid from edges_merge order by ST_Length(geom) desc limit 1)

-- Opearation: Intersects, Description: Largest polygon intersects
-- Query: Given the largest polygon in arealm_merge table, find all lines in edges_merge table that intersect it
select count(*) from  arealm_merge a, edges_merge e where ST_Intersects(e.geom, a.geom) and a.gid= (select gid from arealm_merge order by ST_Area(geom) desc limit 1)

-- Operation: Overlaps, Description: Largest polygon overlaps
-- Query: Given the largest polygon in arealm_merge table, find all polygons in areawater_merge table that overlap it
select count(*) from  arealm_merge al, areawater_merge aw where ST_Overlaps(al.geom, aw.geom) and al.gid= (select gid from arealm_merge order by ST_Area(geom) desc limit 1)

-- Opeariton: Contains, Description: Largest polygon contains 
-- Query: Given the largest polygon in the areawater_merge table, find all points in pointlm_merge table contained by it
select count(*) from  pointlm_merge pl, areawater_merge aw where ST_Contains(aw.geom, pl.geom) and aw.gid= (select gid from areawater_merge order by ST_Area(geom) desc limit 1)

-- Spatial analysis
-- Operation: Distance, Description: Distance search 
-- Query: Find all polygons in arealm merge table that are within 1000 distance units from a given point.
select count(*) from arealm_merge  where Distance (geom, ST_SetSRID(ST_MakePoint(-97.7,30.30), "+SRID+")) < 1000

-- Operation: Within, Description: Bounding box search
-- Query: Find all lines in edges_merge table that are inside the bounding box of a given specification.
select count(*) from edges_merge where Within( geom, ST_SetSRID(PolyFromText('POLYGON((-97.7 30.30, -92.7 30.30, -92.7 27.30, -97.7 27.30, -97.7 30.30))') ,"+SRID+"))
	    
-- Operation: Dimension, Description: Dimension of polygons
-- Query: Find the dimension of all polygons in arealm_merge table
select ST_Dimension(a.geom) from  arealm_merge a

-- Operation: Envelope, Description: Envelope of lines
-- Query: Find the envelopes of the first 1000 lines in edges_merge table
select AsText(ST_Envelope(e.geom)) from edges_merge e limit 1000

-- Operation: Length, Description: Longest line
-- Query: Find the longest line in edges_merge table
SELECT fullname, ST_Length(geom) AS line FROM edges_merge order BY line DESC limit 1

-- Operation: Area, Description: Largest area
-- Query: Find the largest polygon in areawater_merge table
SELECT  fullname, ST_Area(geom) AS area FROM areawater_merge order BY area DESC limit 1

-- Operation: Length, Description: Total line length
-- Query: Determine the total length of all lines in edges_merge table
SELECT sum(ST_Length(geom))/1000 AS km_roads FROM edges_merge

-- Operation: Area, Description: Total area
-- Query: Determine the total area of all polygons in areawater_merge table
SELECT sum(ST_Area(geom)) AS area FROM areawater_merge

-- Operation: Buffer, Description: Buffer of polygons
-- Query: Construct the buffer regions around one mile radius of all polygons in arealm_merge table
select ST_Buffer(a.geom,5280) from  arealm_merge a

-- Operation: ConvexHull, Description: Convex hull of polygons
-- Query: Construct the convex hulls of all polygons in arealm merge table
select ST_ConvexHull(a.geom) from  arealm_merge a

