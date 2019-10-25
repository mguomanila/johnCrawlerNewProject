#!/usr/bin/env python
#-*- coding: utf-8 -*-

from __future__ import print_function

import os
import sys
import traceback

import argparse
import pandas as pd
import datetime
import logging

#------------------------------------------------------------------------------

VERSION = '1'

#------------------------------------------------------------------------------

parser = argparse.ArgumentParser()
parser.add_argument('-i', '--input', default='ksiegi_wieczyste.csv',
                        help='Plik CSV wejsciowy [domyslnie: ksiegi_wieczyste.csv]')
parser.add_argument('-o', '--output', default='output.csv',
                        help='Pliki CSV wyjsciowy [domyslnie: output.csv]')

parser.add_argument('-s', '--sep', default=';',
                        help='Separator kolumn [domyslnie: ;]')

parser.add_argument('--puste', default='', 
                        help='Porzucenie pustych wierszy np. "os_pesel,os_data;os_imie1,os_imie2" daje dwa sprawdzenia "os_pesel,os_data" oraz "os_imie1,os_imie2" [domyslnie: nic]')
                        
parser.add_argument('--duplikaty', default='', 
                        help='Porzucenie duplikatow np. "os_pesel,os_data;os_imie1,os_imie2" daje dwa sprawdzenia "os_pesel,os_data" oraz "os_imie1,os_imie2" [domyslnie: nic]')

parser.add_argument('--kolumny', default='', 
                        help='Porzucenie kolumn np. "os_pesel,os_data" [domyslnie: nic]')

parser.add_argument('-V', '--version', default=False, action='store_true', 
                        help='Numer wersji')

parser.add_argument('-D', '--debug', default=False, action='store_true',
                        help='Debug [domyslnie: NIE]')
                        
args = parser.parse_args()

#------------------------------------------------------------------------------

if args.version:
    print('version: {}'.format(VERSION))
    exit(0)

#------------------------------------------------------------------------------

folder_logs = 'logs'
if not os.path.exists(folder_logs):
    os.makedirs(folder_logs)

log = logging.getLogger()
if args.debug:
    log.setLevel(logging.DEBUG)
else:
    log.setLevel(logging.INFO)

file_formatter = logging.Formatter('%(asctime)s;%(levelname)s;%(message)s', '%Y.%m.%d;%H:%M:%S')
file_handler = logging.FileHandler(datetime.datetime.now().strftime('{}/drop__%Y%m%d__%H%M%S.csv'.format(folder_logs)))
file_handler.setFormatter(file_formatter)
log.addHandler(file_handler)

console_formatter = logging.Formatter('%(asctime)s : %(levelname)s : %(message)s', '%Y.%m.%d : %H:%M:%S')
console_handler = logging.StreamHandler()
console_handler.setFormatter(console_formatter)
log.addHandler(console_handler)

#------------------------------------------------------------------------------

log.debug('args.input: {}'.format(args.input))
log.debug('args.output: {}'.format(args.output))
log.debug('args.sep: {}'.format(args.sep))
log.debug('args.puste: {}'.format(args.puste))
log.debug('args.duplikaty: {}'.format(args.duplikaty))
log.debug('args.kolumny: {}'.format(args.kolumny))

#------------------------------------------------------------------------------

try:
    if args.puste:
        args.puste = [x.strip() for x in args.puste.split(';')]
except Exception as ex:
    log.error(ex)        
    log.error('Porzucanie pustych nie bedzie zastosowane')
    args.puste = None

#------------------------------------------------------------------------------
    
try:
    if args.duplikaty:
        args.duplikaty = [x.strip() for x in args.duplikaty.split(';')]
except Exception as ex:
    log.error(ex)        
    log.error('Porzucanie duplikatow nie bedzie zastosowane')
    args.duplikaty = None
        
#------------------------------------------------------------------------------
    
try:
    if args.kolumny:
        args.kolumny = [x.strip() for x in args.kolumny.split(';')]
except Exception as ex:
    log.error(ex)        
    log.error('Porzucanie kolumn nie bedzie zastosowane')
    args.kolumny = None
        
#------------------------------------------------------------------------------

df = pd.read_csv(args.input, sep=args.sep, dtype=str)

#------------------------------------------------------------------------------

try:
    if args.puste:
        for items in args.puste:
            if ',' in items:
                items = [x.strip() for x in items.split(',')]
            ilosc_przed = len(df)
            df = df[ df[items].notnull() ]
            ilosc_po = len(df)
            log.info('puste: {} (ilosc: {})'.format(items, ilosc_przed-ilosc_po))
except Exception as ex:
    log.error('puste: {}'.format(ex))

#------------------------------------------------------------------------------

try:
    if args.duplikaty:
        for items in args.duplikaty:
            items = [x.strip() for x in items.split(',')]
            ilosc_przed = len(df)
            df.drop_duplicates(subset=items, inplace=True)
            ilosc_po = len(df)
            log.info('duplikaty: {} (ilosc: {})'.format(items, ilosc_przed-ilosc_po))
except Exception as ex:
    log.error('duplikaty: {}'.format(ex))

#------------------------------------------------------------------------------

try:
    if args.kolumny:
        for items in args.kolumny:
            items = [x.strip() for x in items.split(',')]
            ilosc_przed = len(df.columns)
            df.drop(labels=items, axis='columns', inplace=True)
            ilosc_po = len(df.columns)
            log.info('kolumny: {} (ilosc: {})'.format(items, ilosc_przed-ilosc_po))
except Exception as ex:
    log.error('kolumny: {}'.format(ex))
    
#------------------------------------------------------------------------------

df.to_csv(args.output, sep=args.sep, index=False)

