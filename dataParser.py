#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Nov 20 17:13:24 2019

@author: matthew
"""
import gzip
from collections import OrderedDict


# noinspection PyMethodMayBeStatic
class dataParser:
    def __init__(self, inputDir):
        self.__INPUT_DIRECTORY = inputDir
        self.__REFERENCE_DIRECTORY = inputDir + '/reference/'

    # open stations file and return stream
    def getStations(self):
        stream = ''
        with open(self.__REFERENCE_DIRECTORY + 'ghcnd-stations.txt', 'r') as file:
            indices = OrderedDict()
            indices['ID'] = [0, 11]
            indices['LATITUDE'] = [12, 20]
            indices['LONGITUDE'] = [21, 30]
            indices['ELEVATION'] = [31, 37]
            indices['STATE'] = [38, 40]
            indices['NAME'] = [41, 71]
            indices['GSN FLAG'] = [72, 75]
            indices['HCN/CRN FLAG'] = [76, 79]
            indices['WMO:ID'] = [80, 85]

            # create csv file stream 
            for line in file:
                row = ''
                for key, value in indices.items():
                    start = value[0]
                    end = value[1]
                    row += line[start:end].strip()

                    if key == 'WMO:ID':
                        row += '\n'
                    else:
                        row += ','

                stream += row
        return stream

    # open country file and return stream
    def getCountries(self):
        stream = ''

        with open(self.__REFERENCE_DIRECTORY + 'ghcnd-countries.txt', 'r') as file:
            for line in file:
                countryId = line[:2]
                countryName = line[3:]
                stream += (countryId + ',' + countryName.strip() + '\n')

        return stream

    # open geoname file and return stream as a list of rows
    def getGeonames(self):
        with open(self.__REFERENCE_DIRECTORY + 'geonames.tsv', 'r') as file:
            stream = list()
            line = file.readline()
            for line in file:
                if len(line) > 1:
                    row = line.strip().split('\t')
                    stream.append(row)

        return stream

