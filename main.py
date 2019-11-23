#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Nov 13 16:56:45 2019

@author: matthew
"""
import sys, getopt
import gcis


def main():        
    try:
        args = sys.argv[1:]
        
        gcis_instance = gcis()
        # Initial Data Load 
            # FTP
            # Parse Data
            # Load data
        
        # Routine Database Update 
            # FTP
            # Filter only needed data
            # Parse data
            # Load new data
        
        # Add new set of geonames 
        
        # Add new elements to be imported to database
        
    
    except getopt.error as argError:
        print(str(argError))



if __name__ == "__main__":
    main()
    

