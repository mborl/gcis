#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Nov 13 17:49:37 2019

@author: matthew

This script describes an object which loads already parsed files to the GCIS database 
"""
import gzip
import os
import psycopg2
import re

from dataParser import dataParser


class dataLoader:
    def __init__(self, inputDir, elements, db_host, db_user, db_pwd, db):
        self.__INPUT_DIRECTORY = inputDir
        self.__ELEMENTS = set(elements)
        self.__HOST = db_host
        self.__USER = db_user
        self.__PASSWORD = db_pwd
        self.__DATABASE = db

    def initialLoad(self):
        conn = psycopg2.connect(host=self.__HOST, database=self.__DATABASE, user=self.__USER, password=self.__PASSWORD)
        cursor = conn.cursor()

        # Create gcis schema
        create = """
        create schema gcis;

        alter schema gcis owner to {};

        create table "Country"
        (
            "CountryID" varchar(20) not null
                constraint "Country_pkey"
                    primary key,
            "CountryName" varchar(100) not null
        );
        
        alter table "Country" owner to {};
        
        create table "Station"
        (
            "StationKey" serial not null
                constraint "Station_pkey"
                    primary key,
            "StationID" varchar(50) not null,
            "StationName" varchar(100),
            "SLongitude" real not null,
            "SLatitude" real not null,
            "SElevation" real not null,
            "CountryID" varchar(2) not null
                constraint country___fk
                    references "Country"
                        on update cascade
        );
        
        alter table "Station" owner to {}};
        
        create table "Climate"
        (
            "ClimateID" serial not null
                constraint "Climate_pkey"
                    primary key,
            "StationID" varchar(20) not null
                constraint station__fk
                    references "Station" ("StationID")
                        on update cascade on delete cascade,
            "DATE" date not null,
            "ELEMENT" varchar(10) not null,
            "VALUE" integer not null
        );
        
        alter table "Climate" owner to {}};
        
        create unique index station_stationid_uindex
            on "Station" ("StationID");
        
        create table "Geoname"
        (
            "GeonameID" integer not null
                constraint geoname_pk
                    primary key,
            "LocationName" varchar(100),
            "CountryID" varchar(5)
                constraint country__fk
                    references "Country"
                        on update cascade on delete restrict,
            "Latitude" integer,
            "Longitude" integer,
            "Elevation" integer
        );
        
        alter table "Geoname" owner to {}};
        
        create unique index "Geoname_""GeonameID""_uindex"
            on "Geoname" ("GeonameID");
        """.format(self.__USER)

        cursor.execute(create)
        conn.commit()

        cursor.close
        conn.close()

        # populate tables
        self.loadAll()

    def loadAll(self):
        self.load_countries()
        print('Countries successfully loaded')
        self.load_stations()
        print('Stations successfully loaded')
        self.loadGeonames()
        print('Geonames successfully loaded')
        self.load_climate_data()
        print('Climate Data successfully loaded')

    def truncateTables(self):
        conn = psycopg2.connect(host=self.__HOST, database=self.__DATABASE, user=self.__USER, password=self.__PASSWORD)
        cursor = conn.cursor()
        tableNames = ["Climate", "Geoname", "Country", "Station"]
        for table in tableNames:
            cursor.execute("""TRUNCATE TABLE gcis."{}";""".format(table))
            conn.commit()
        cursor.close
        conn.close()

        print('Tables successfully truncated')
        self.loadAll()

    def load_climate_data(self):
        path = self.__INPUT_DIRECTORY
        for file in os.listdir(path):     # loop through files
            if re.match(r'^.*(.csv.gz)$', file):    # get all csv.gz files
                with gzip.open('{path}/{file}'.format(path=path, file=file), 'rb') as f:    # open the file
                    count = 0         # keeps track of number insert statements created
                    insertBlock = ''

                    for line in f:
                        if len(line) > 1:
                            line = line.decode().split(',')
                            element = line[2]
                            if element in self.__ELEMENTS:
                                stationID = line[0]
                                rawDate = line[1]
                                climateDate = '{year}-{month}-{day}'.format(year=rawDate[:4], month=rawDate[4:6], day=rawDate[6:8])
                                value = line[3]

                                insertBlock += """INSERT INTO gcis."Climate" ("StationID", "DATE", "ELEMENT", "VALUE")\
                                VALUES ('{id}', '{date}', '{el}', {val});\n""".format(
                                    id=stationID,
                                    date=climateDate,
                                    el=element,
                                    val=value
                                )

                                if count < 100000:
                                    count += 1
                                else:
                                    self.pushClimateToDB(insertBlock)
                                    insertBlock = ''
                                    count = 0

            print('{} successfully loaded to GCIS Database'.format(file))

    def pushClimateToDB(self, inserts):
        # insert into climate table
        conn = psycopg2.connect(host=self.__HOST, database=self.__DATABASE, user=self.__USER,
                                password=self.__PASSWORD)
        cursor = conn.cursor()
        cursor.execute(inserts)
        conn.commit()
        cursor.close
        conn.close()

    def load_stations(self):
        # get station file stream
        parser = dataParser(self.__INPUT_DIRECTORY)
        stationStream = parser.getStations().split('\n')
        inserts = ''

        # create insert statements 
        for line in stationStream:
            row = line.strip().split(',')
            if row != ['']:
                stationID = "'{}'".format(row[0])
                lat = row[1]
                lon = row[2]
                elev = row[3]
                stationName = "'{}'".format(re.sub("'", '', row[5]))
                country = "'{}'".format(re.sub("'", '', stationID[1:3]))

                inserts += '''INSERT INTO gcis."Station" ("StationID", "StationName", "SLongitude", "SLatitude", "SElevation", "CountryID") 
                VALUES ({id}, {name}, {longitude}, {latitude}, {elevation}, {countryID});\n'''.format(id=stationID,
                                                                                                      name=stationName,
                                                                                                      longitude=lon,
                                                                                                      latitude=lat,
                                                                                                      elevation=elev,
                                                                                                      countryID=country)

        # insert into stations table 
        conn = psycopg2.connect(host=self.__HOST, database=self.__DATABASE, user=self.__USER, password=self.__PASSWORD)
        cursor = conn.cursor()
        cursor.execute(inserts)
        conn.commit()
        cursor.close
        conn.close()

    def load_countries(self):
        parser = dataParser(self.__INPUT_DIRECTORY)
        countriesData = parser.getCountries().split('\n')
        inserts = ''

        for line in countriesData:
            row = line.split(',')
            if row != ['']:
                countryID = "'{}'".format(row[0])
                countryName = "'{}'".format(re.sub("'", '', row[1]))

                inserts += '''INSERT INTO gcis."Country" ("CountryID", "CountryName") VALUES ({ID}, {name});'''.format(
                    ID=countryID, name=countryName)

        conn = psycopg2.connect(host=self.__HOST, database=self.__DATABASE, user=self.__USER, password=self.__PASSWORD)
        cursor = conn.cursor()
        cursor.execute(inserts)
        conn.commit()
        cursor.close
        conn.close()

    def loadGeonames(self):
        parser = dataParser(self.__INPUT_DIRECTORY)
        gRows = parser.getGeonames()

        inserts = ""
        for row in gRows:
            print(row)
            geonameID = row[0]
            gLoc = "'{}'".format(re.sub("'", '', row[1].strip()))  # remove apostrophes from location names
            gCountry = "'{}'".format(row[2])
            gLat = row[3]
            gLon = row[4]
            gElev = row[5]  # None if row[5].upper() == 'NULL' else row[5]


            inserts += """INSERT INTO gcis."Geoname" ("GeonameID", "LocationName", "CountryID", "Latitude", "Longitude", "Elevation")\
             VALUES ({id}, {loc}, {country}, {lat}, {lon}, {elev});\n""".format(
                id=geonameID, loc=gLoc, country=gCountry, lat=gLat, lon=gLon, elev=gElev
            )

        conn = psycopg2.connect(host=self.__HOST, database=self.__DATABASE, user=self.__USER, password=self.__PASSWORD)
        cursor = conn.cursor()
        cursor.execute(inserts)
        conn.commit()
        cursor.close
        conn.close()