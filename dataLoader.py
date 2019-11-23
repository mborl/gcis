#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Nov 13 17:49:37 2019

@author: matthew

This script describes an object which loads already parsed files to the GCIS database 
"""
import psycopg2
from dataParser import dataParser 
import re

class dataLoader():
    def __init__(self, inputDir, elements, db_host, db_user, db_pwd, db):
        self.__INPUT_DIRECTORY = inputDir
        self.__ELEMENTS = elements
        self.__HOST = db_host
        self.__USER = db_user
        self.__PASSWORD = db_pwd
        self.__DATABASE = db
   

    def initialLoad(self):
        self.load_countries()
        self.load_stations()
    
    
    
    def load_climate_data(self):
        # get year file stream
        
        # insert into climate table 
        pass
    
    
    
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
                stationName = "'{}'".format(re.sub("'", '',row[5]))
                country = "'{}'".format(re.sub("'", '', stationID[1:3]))
            
                inserts += '''INSERT INTO gcis."Station" ("StationID", "StationName", "SLongitude", "SLatitude", "SElevation", "CountryID") 
                VALUES ({id}, {name}, {longitude}, {latitude}, {elevation}, {countryID});\n'''.format(id=stationID, name=stationName, longitude=lon, latitude=lat, elevation=elev, countryID=country)
            
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
                
                inserts += '''INSERT INTO gcis."Country" ("CountryID", "CountryName") VALUES ({ID}, {name});'''.format(ID=countryID, name=countryName)
        
        conn = psycopg2.connect(host=self.__HOST, database=self.__DATABASE, user=self.__USER, password=self.__PASSWORD)
        cursor = conn.cursor()
        cursor.execute(inserts)
        conn.commit()
        cursor.close
        conn.close()