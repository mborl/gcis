#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Oct 12 12:56:21 2019

@author: matthew
"""
import pandas as pd 

class dlyParser():
    
    # Description: Constructor 
    # List() elements: the elements desired by user (obtained from config file)
    def __init__(self, elements):
       self.__INIT_COLS = [(0,11), (11,15), (15,17), (17,21)]
       self.__values = list() 
       self.__elements = elements
       
       # predefined lengths of data elements according to dly README
       self.__NUM_DAYS = 31
       self.__VALUE = 5                                                    # length of value element 
       self.__MFLAG = 1                                                    # length of MFLAG element
       self.__QFLAG = 1
       self.__SFLAG = 1
       self.__VARIABLE_LENGTHS = [self.__VALUE, self.__MFLAG, self.__QFLAG, self.__SFLAG]


    # Description: Gets the indices of the values that need to be extracted 
    def getDayDataIndices(self):
        cursor = 21                                                  # location of VALUE1 (according to ghcnd readme)       
        values = list()                                              
    
        for day in range(1, self.__NUM_DAYS + 1, 1):                   # loop through each day (each day has a VALUE, MFLAG, QFLAG, and SFLAG)
            for element in self.__VARIABLE_LENGTHS:
                elementRange  = (cursor, cursor + element)             # tuple of starting (inclusive) and ending (exclusive) indices 
                values.append(elementRange)
                cursor  = cursor + element
        
        return values
   
    
    # Description: Creates a list of column names for the dataframe produced in the parse() method 
    def generate_dly_header(self):
        initialColumns = ['ID', 'YEAR', 'MONTH', 'ELEMENT']
        valueColumns = list()
        
        counter = 1
        for day in range(1, self.__NUM_DAYS + 1, 1):                   # loop through each day (each day has a VALUE, MFLAG, QFLAG, and SFLAG)
            current = ['VALUE' + str(counter), 'MFLAG' + str(counter), 'QFLAG' + str(counter), 'SFLAG' + str(counter)]
            valueColumns += current
            counter += 1
        
        header = initialColumns + valueColumns
        return header
        
    # Description: parses the dly file to a pandas dataframe 
    def parse(self, inputFile):
       self.__values = self.getDayDataIndices()
       finalCols = self.__INIT_COLS + self.__values   
       dataHeader = self.generate_dly_header()
       data = pd.read_fwf(inputFile, colspecs=finalCols, names=dataHeader)
       
       return data

#
#testInstance = dlyParser(['PRCP','SNOW','TMAX','TMIN','TAVG','TSUN','AWDR','AWND'])
#
#test = testInstance.parse('/home/matthew/data/ghcnd_all/US1WASP0033.dly')