DROP FUNCTION IF EXISTS gcis.calculateDistance(gID INT, sID VARCHAR(50));

CREATE FUNCTION gcis.calculateDistance(gID INT, sID VARCHAR(50)) RETURNS REAL AS $$
    DECLARE
        gLat REAL;                  -- latitude of geoname
        gLon REAL;                  -- longitude of geoname

        sLat REAL;                  -- latitude of station
        sLon REAL;                  -- longitude of station
        distance REAL;              -- distance between geoname and station

        -- variables needed for distance calculation
        dLat REAL;
        dLon REAL;
        x REAL;
        y REAL;
        EARTH_RADIUS CONSTANT REAL  := 6371;
    BEGIN
        -- get geographical information for geoname
        SELECT "Latitude", "Longitude"
        FROM gcis."Geoname"
        WHERE "GeonameID" = gID
        INTO gLat, gLon;

        -- get geographical information for station
        SELECT "SLatitude", "SLongitude"
        FROM gcis."Station"
        WHERE "StationID" = sID
        INTO sLat, sLon;

        -- calculation of distance using Haversine formula
        -- (source: https://www.geeksforgeeks.org/program-distance-two-points-earth/)
        sLat = RADIANS(sLat);
        sLon = RADIANS(sLon);
        gLat = RADIANS(gLat);
        gLon = RADIANS(gLon);

        dLon = sLon - gLon;
        dLat = sLat - gLat;
        x = (sin(dlat / 2)^2) + (cos(sLat) * cos(gLat) * sin(dlon / 2)^2);

        y = 2 * asin(|/x);
        distance = y * EARTH_RADIUS;

        RETURN distance;

    END;
$$

LANGUAGE PLPGSQL;