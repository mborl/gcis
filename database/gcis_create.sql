CREATE SCHEMA gcis;
        
        
        ALTER SCHEMA gcis OWNER TO {};
        
        --
        -- Name: calculatedistance(integer, character varying); Type: FUNCTION; Schema: gcis; Owner: {}
        --
        
        CREATE FUNCTION gcis.calculatedistance(gid integer, sid character varying) RETURNS real
            LANGUAGE plpgsql
            AS $$
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
        $$;
        
        
        ALTER FUNCTION gcis.calculatedistance(gid integer, sid character varying) OWNER TO {};
        
        CREATE FUNCTION gcis.findstation(id integer) RETURNS character varying
            LANGUAGE plpgsql
            AS $$
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
            END;  $$;
        
        
        ALTER FUNCTION gcis.findstation(id integer) OWNER TO {};
        
        SET default_tablespace = '';
        
        SET default_with_oids = false;
        
        --
        -- Name: Climate; Type: TABLE; Schema: gcis; Owner: {}
        --
        
        CREATE TABLE gcis."Climate" (
            "ClimateID" integer NOT NULL,
            "StationID" character varying(20) NOT NULL,
            "DATE" date NOT NULL,
            "ELEMENT" character varying(10) NOT NULL,
            "VALUE" integer NOT NULL
        );
        
        
        ALTER TABLE gcis."Climate" OWNER TO {};
        
        --
        -- Name: Climate_ClimateID_seq; Type: SEQUENCE; Schema: gcis; Owner: {}
        --
        
        CREATE SEQUENCE gcis."Climate_ClimateID_seq"
            AS integer
            START WITH 1
            INCREMENT BY 1
            NO MINVALUE
            NO MAXVALUE
            CACHE 1;
        
        
        ALTER TABLE gcis."Climate_ClimateID_seq" OWNER TO {};
        
        --
        -- Name: Climate_ClimateID_seq; Type: SEQUENCE OWNED BY; Schema: gcis; Owner: {}
        --
        
        ALTER SEQUENCE gcis."Climate_ClimateID_seq" OWNED BY gcis."Climate"."ClimateID";
        
        
        --
        -- Name: Country; Type: TABLE; Schema: gcis; Owner: {}
        --
        
        CREATE TABLE gcis."Country" (
            "CountryID" character varying(20) NOT NULL,
            "CountryName" character varying(100) NOT NULL
        );
        
        
        ALTER TABLE gcis."Country" OWNER TO {};
        
        --
        -- Name: Geoname; Type: TABLE; Schema: gcis; Owner: {}
        --
        
        CREATE TABLE gcis."Geoname" (
            "GeonameID" integer NOT NULL,
            "LocationName" character varying(100),
            "CountryID" character varying(5),
            "Latitude" integer,
            "Longitude" integer,
            "Elevation" integer
        );
        
        
        ALTER TABLE gcis."Geoname" OWNER TO {};
        
        --
        -- Name: Station; Type: TABLE; Schema: gcis; Owner: {}
        --
        
        CREATE TABLE gcis."Station" (
            "StationKey" integer NOT NULL,
            "StationID" character varying(50) NOT NULL,
            "StationName" character varying(100),
            "SLongitude" real NOT NULL,
            "SLatitude" real NOT NULL,
            "SElevation" real NOT NULL,
            "CountryID" character varying(2) NOT NULL
        );
        
        
        ALTER TABLE gcis."Station" OWNER TO {};
        
        --
        -- Name: Station_StationKey_seq; Type: SEQUENCE; Schema: gcis; Owner: {}
        --
        
        CREATE SEQUENCE gcis."Station_StationKey_seq"
            AS integer
            START WITH 1
            INCREMENT BY 1
            NO MINVALUE
            NO MAXVALUE
            CACHE 1;
        
        
        ALTER TABLE gcis."Station_StationKey_seq" OWNER TO {};
        
        --
        -- Name: Station_StationKey_seq; Type: SEQUENCE OWNED BY; Schema: gcis; Owner: {}
        --
        
        ALTER SEQUENCE gcis."Station_StationKey_seq" OWNED BY gcis."Station"."StationKey";
        
        
        --
        -- Name: Climate ClimateID; Type: DEFAULT; Schema: gcis; Owner: {}
        --
        
        ALTER TABLE ONLY gcis."Climate" ALTER COLUMN "ClimateID" SET DEFAULT nextval('gcis."Climate_ClimateID_seq"'::regclass);
        
        
        --
        -- Name: Station StationKey; Type: DEFAULT; Schema: gcis; Owner: {}
        --
        
        ALTER TABLE ONLY gcis."Station" ALTER COLUMN "StationKey" SET DEFAULT nextval('gcis."Station_StationKey_seq"'::regclass);
