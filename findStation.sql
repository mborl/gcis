DROP FUNCTION IF EXISTS gcis.findstation(id integer);

CREATE FUNCTION findStation(id INT) RETURNS VARCHAR(50) AS $$
    DECLARE
        gCountry VARCHAR(100);      -- country of geoname
        nearestStation VARCHAR(50); -- current nearest station

    BEGIN
        -- narrow search of stations to appropriate country
        SELECT "CountryID"
        FROM gcis."Geoname"
        WHERE "GeonameID" = id
        INTO gCountry;

        SELECT "StationID", MIN(distance)
        FROM (
            SELECT "StationID", calculatedistance(id, "StationID") AS distance
            FROM gcis."Station" s
            WHERE "CountryID" = gCountry
        ) distances
        GROUP BY "StationID", distance
        ORDER BY distance ASC
        LIMIT 1
        INTO nearestStation;


        RETURN nearestStation;
    END;  $$

LANGUAGE PLPGSQL;
