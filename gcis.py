#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Nov 14 15:44:49 2019

@author: matthew
"""
#import dlyParser, dataLoader, ghcnInterface, geonameInserter
from ghcnInterface   import ghcnInterface
from dataLoader      import dataLoader
from geonameInserter import geonameInserter



class gcis:
    def __init__(self):
        self.__configPath = '.config'
        
        # variables from config file 
        self.__ELEMENTS            = None
        self.__DB_HOST             = None
        self.__DB_USER             = None
        self.__DB_PASSWORD         = None
        self.__DB_SCHEMA           = None
        self.__INPUT_DIRECTORY     = None
        self.__FTP_HOST            = None
        self.__FTP_INPUT_DIRECTORY = None
        
        self.loadConfig()
        
        self._interface         = ghcnInterface(self.__FTP_HOST, self.__FTP_INPUT_DIRECTORY, self.__INPUT_DIRECTORY)
#        self.parser             = dlyParser(self.__ELEMENTS)
        self._dbLoader          = dataLoader(self.__INPUT_DIRECTORY, self.__ELEMENTS, self.__DB_HOST, self.__DB_USER, self.__DB_PASSWORD, self.__DB_SCHEMA)
        self._newLocationLoader = geonameInserter()
        
    
    def loadConfig(self):
        with open(self.__configPath) as config:
            for line in config:
                line = line.strip()
                parsedVariables = line.split('=')
                
                if len(parsedVariables) == 2 and line[0] != '#':
                    variable = parsedVariables[0].strip()
                    value = parsedVariables[1].strip()
                    
                    if variable == 'ELEMENTS':
                        self.__Elements = value.strip().split(',')
                    
                    elif variable == 'DB_HOST':
                        self.__DB_HOST = value

                    elif variable == 'DB_USER':
                        self.__DB_USER = value
                    
                    elif variable == 'DB_PASSWORD':
                        self.__DB_PASSWORD = value
                    
                    elif variable == 'DB_SCHEMA':
                        self.__DB_PASSWORD = value
                    
                    elif variable == 'INPUT_DIRECTORY':
                        self.__INPUT_DIRECTORY = value
                    
                    elif variable == 'FTP_HOST':
                        self.__FTP_HOST = value
                    
                    elif variable == 'FTP_INPUT_DIRECTORY':
                        self.__FTP_INPUT_DIRECTORY = value
                    
                    else:
                        raise InvalidConfigFormat

    
    # Initialize first load of data; uses dataLoader  
    def init(self):
        pass
    
    
    
    # update GCIS database with latest GHCN data; uses ghcnInterface and dataLoader
    def update_local(self):
        confirm = input('Confirm update: This will DELETE all files in your input directory (y/n)\n')
        
        if confirm.strip().lower() == 'y':
            self._interface.update_local_ghcn()
            print('Update Complete')
        elif confirm.strip().lower() == 'n':
            print('Update cancelled')
        else:
            print('Unrecognized input')
    
    
    # truncate tables and then repopulate tables 
    def update_db(self):
        # update stations
        
        # update countries
        
        # update climate data
        
        pass
    
    
    # add new geonames; uses geonameInserter 
    def addNewGeonames(self):
        pass
    
    
    
    # add new GHCN Climate data elements 
    def addElementsToDB(self):
        pass
    
    

class InvalidConfigFormat(Exception):
    pass


gcisInstance = gcis()
gcisInstance.update_local()
