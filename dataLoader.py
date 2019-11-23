#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Nov 13 17:49:37 2019

@author: matthew

This script describes an object which loads already parsed files to the GCIS database 
"""
import psycopg2
from dataParser import dataParser 

class dataLoader():
    def __init__(self, inputDir, elements, db_host, db_user, db_pwd, db_schema):
        self.__INPUT_DIRECTORY = inputDir
        self.__ELEMENTS = elements
        self.__HOST = db_host
        self.__USER = db_user
        self.__PASSWORD = db_pwd
        self.__SCHEMA = db_schema
   

    def initialLoad(self):
        pass
    
    
    def load_climate_data(self):
        # get year file stream
        
        # insert into climate table 
        pass
    
    
    def load_stations(self):
        # get station file stream
        parser = dataParser(self.__INPUT_DIRECTORY)
        stationStream = dataParser.getStations()

        # create insert statements 
        QUERY = r'INSERT INTO stations ()'

        # insert into stations table 
        conn = psycopg2.connect(host=self.__HOST, database=self.__SCHEMA, user=self.__USER, password=self.__PASSWORD)
        cursor = conn.cursor()
        cur.close
        conn.close()
    
    def load_countries(self):
        pass 


#import psycopg2
#cur = conn.cursor()
#cur.execute("CREATE TABLE GCIS.test (id serial PRIMARY KEY, num integer, data varchar);")
#conn.commit()
#cur.close
#conn.close()
