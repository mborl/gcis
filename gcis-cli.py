#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Nov 13 16:56:45 2019

@author: matthew
"""
import sys
import getopt
from gcis import gcis


def main():
    try:
        args = sys.argv[1:]
        HELP_MESSAGE = """\
        Usage:
            gcis-cli [argument]
            -h help
            -i initialize
            -u [local/db]
                local: update local ghcn files
                db: update Postgres GCIS database
        """
        gcis_instance = gcis()
        # Initial Data Load 
        if args[0] == '-h':
            print(HELP_MESSAGE)

        elif args[0] == '-i':
            gcis_instance.init()

        elif args[0] == '-u':
            if args[1] == 'local':
                gcis_instance.update_local_ghcn()
            elif args[1] == 'db':
                gcis_instance.update_db()

    except getopt.error as argError:
        print(str(argError))
        print(HELP_MESSAGE)

    except IndexError as indexError:
        print(HELP_MESSAGE)

if __name__ == "__main__":
    main()
