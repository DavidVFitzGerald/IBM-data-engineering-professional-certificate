SELECT
    f.stationid, t.trucktype, SUM(f.wastecollected) AS totalwastecollected
FROM
    FactTrips AS f
LEFT JOIN
    DimTruck AS t ON f.truckid = t.truckid
GROUP BY GROUPING SETS (
    (f.stationid, t.trucktype),
    f.stationid,
    t.trucktype,
    ()
)
ORDER BY
    f.stationid,
    t.trucktype
;


SELECT
    d.year, s.city, s.stationid, SUM(f.wastecollected) AS totalwastecollected
FROM
    FactTrips AS f
LEFT JOIN
    DimStation AS s ON f.stationid = s.stationid
LEFT JOIN
	DimDate AS d ON f.dateid = d.dateid
GROUP BY ROLLUP (d.year, s.city, s.stationid)
ORDER BY
    d.year DESC,
	s.city,
	s.stationid
;


SELECT
    d.year, s.city, s.stationid, AVG(f.wastecollected) AS averagewastecollected
FROM
    FactTrips AS f
LEFT JOIN
    DimStation AS s ON f.stationid = s.stationid
LEFT JOIN
	DimDate AS d ON f.dateid = d.dateid
GROUP BY CUBE (d.year, s.city, s.stationid)
ORDER BY
    d.year DESC,
	s.city,
	s.stationid
;


CREATE MATERIALIZED VIEW max_waste_stats AS (
SELECT
    s.city, s.stationid, t.trucktype, MAX(f.wastecollected) AS maxwastecollected
FROM
    FactTrips AS f
LEFT JOIN
    DimStation AS s ON f.stationid = s.stationid
LEFT JOIN
    DimTruck AS t ON f.truckid = t.truckid
GROUP BY (s.city, s.stationid, t.trucktype)
ORDER BY
    s.city,
    s.stationid,
    t.trucktype
);
