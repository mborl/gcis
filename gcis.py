#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Nov 14 15:44:49 2019

@author: matthew
"""
# import dlyParser, dataLoader, ghcnInterface, geonameInserter
from ghcnInterface import ghcnInterface
from dataLoader import dataLoader


class gcis:
    def __init__(self):
        self.__configPath = '.config'

        # variables from config file 
        self.__ELEMENTS = None
        self.__DB_HOST = None
        self.__DB_USER = None
        self.__DB_PASSWORD = None
        self.__DB_CREATE = None
        self.__DATABASE = None
        self.__INPUT_DIRECTORY = None
        self.__FTP_HOST = None
        self.__FTP_INPUT_DIRECTORY = None

        self.loadConfig()

        self._interface = ghcnInterface(self.__FTP_HOST, self.__FTP_INPUT_DIRECTORY, self.__INPUT_DIRECTORY)
        self._dbLoader = dataLoader(self.__INPUT_DIRECTORY, self.__ELEMENTS, self.__DB_HOST, self.__DB_USER,
                                    self.__DB_PASSWORD, self.__DATABASE, self.__DB_CREATE)

    def loadConfig(self):
        with open(self.__configPath) as config:
            for line in config:
                line = line.strip()
                parsedVariables = line.split('=')

                if len(parsedVariables) == 2 and line[0] != '#':
                    variable = parsedVariables[0].strip()
                    value = parsedVariables[1].strip()

                    if variable == 'ELEMENTS':
                        self.__ELEMENTS = value.strip().split(',')

                    elif variable == 'DB_HOST':
                        self.__DB_HOST = value

                    elif variable == 'DB_USER':
                        self.__DB_USER = value

                    elif variable == 'DB_PASSWORD':
                        self.__DB_PASSWORD = value

                    elif variable == 'DB_CREATE':
                        self.__DB_CREATE = value

                    elif variable == 'DATABASE':
                        self.__DATABASE = value

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
        self._interface.update_local_ghcn()  # create local copy of ghcn files
        self._dbLoader.initialLoad()  # create gcis database and load data

    # update GCIS database with latest GHCN data; uses ghcnInterface and dataLoader
    def update_local_ghcn(self):
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
        confirm = input('Confirm update: This will DELETE ALL DATA in the GCIS Database (y/n)\n')
        if confirm.strip().lower() == 'y':
            self._dbLoader.truncateTables()
            self._dbLoader.loadAll()
        elif confirm.strip().lower() == 'n':
            print('Database update cancelled')
        else:
            print('Unrecognized input')

    # rebuild schema but do not load data
    def rebuild_simple(self):
        self._dbLoader.deleteSchema()
        self._dbLoader.createSchema()

    # rebuild schema and load data
    def rebuild_full(self):
        self._dbLoader.deleteSchema()
        self._dbLoader.initialLoad()


class InvalidConfigFormat(Exception):
    pass
