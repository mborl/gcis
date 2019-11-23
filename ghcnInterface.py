#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Nov 14 16:36:27 2019

@author: matthew

This script creates an object that can be used to get updates from GHCN
"""
from ftplib import FTP
import re 
import os 

class ghcnInterface:
    def __init__(self, FTP, FTP_DIR, INPUT_DIR):
        self.__FTP       = FTP
        self.__FTP_DIR   = FTP_DIR
        self.__INPUT_DIR = INPUT_DIR
        
    
    def update_local_ghcn(self):
        # delete local files to make room for updated files 
        oldFiles = os.listdir(self.__INPUT_DIR)
        for file in oldFiles:
            os.remove(self.__INPUT_DIR + '/' + file)
        
        # create FTP connection to GHCN 
        ftp = FTP(self.__FTP) 
        ftp.login()
        ftp.cwd(self.__FTP_DIR)
        
        # get CSV files from GHCN
        ftpFilenames = ftp.nlst()
        relevantFiles = [file for file in ftpFilenames if re.match(r'20\d{2}.csv.gz', file)] # only get files from year 2000 and later
        self.download_ghcn(relevantFiles, ftp)
        
        ftp.close()
    
    
    
    # download the ghcn files 
    # files: the list of files desired to be downloaded
    # ftp:   the ftp connection being used to download the files
    def download_ghcn(self, files, ftp):
        for filename in files:
            with open(self.__INPUT_DIR + '/' + filename, 'wb') as localFile:
                ftp.retrbinary('RETR ' + filename, localFile.write)
            
            print('%s Successfully Downloaded' %(filename))
    