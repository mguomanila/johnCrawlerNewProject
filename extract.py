#!/usr/bin/env python
#-*- coding: utf-8 -*-

from __future__ import print_function

import os
import sys
import copy # powielanie `record` aby powielac wiersze dla roznych osob, podmiotow i hipotek

import re
import datetime
import pandas as pd
import csv

import bs4
from bs4 import BeautifulSoup
from io import open

import argparse  # uruchamianie z parametrami
import traceback # wyswietlanie calego `Traceback` w `except:`
import logging   # zapisywanie logu do pliku w czasie dzialania skryptu a nie pod jego koniec

#------------------------------------------------------------------------------
# usuniecie ostrzezenia pandas, ze funkcje moga nie istniec w nastepnej wersji
#------------------------------------------------------------------------------

import warnings
warnings.filterwarnings('ignore')

#------------------------------------------------------------------------------

VERSION = '9'

#------------------------------------------------------------------------------
### LISTA KOLUMN DO ZAPISU ####################################################
#------------------------------------------------------------------------------

# lista kolumn wykorzystywana przy zapisywaniu danych do istniejacego pliku - aby zachowac kolejnosc kolumn
COLUMNS = [
    'id', 'plik_io', 'plik_ii', 'plik_iv', 'numer_ksiegi_wieczystej',
    'miejscowosc', 'ulica', 'numer_budynku', 'numer_lokalu',
    'przeznaczenie_lokalu', 'liczba_pokoi', 'kondygnacja', 'pole_powierzchni_uzytkowej',
    'p_nazwa_podmiotu', 'p_miejscowosc_podmiotu', 'p_regon',
    'os_imie1', 'os_imie2', 'os_nazwisko', 'os_ojciec', 'os_matka', 'os_pesel', 'os_data_urodzenia', 'os_rodzaj_wspolnosci',
    'rozdzielnosc_majatkowa',
    'h_rodzaj', 'h_suma', 'h_waluta', 'h_osoba_prawna',
    'h_numer_udzialu_w_prawie', 'os_numer_udzialu_w_prawie',
    'wojewodztwo'
]

# property_types_in
typy_nieruchomosci_in = [
    u'STANOWIĄCY ODRĘBNĄ NIERUCHOMOŚĆ',
    u'PRAWO DO LOKALU',
    u'NIERUCHOMOŚĆ GRUNTOWA',
    u'GRUNT ODDANY W UŻYTKOWANIE WIECZYSTE',
]
# typy_nieruchomosci_out = [u'NIERUCHOMOŚĆ GRUNTOWA', u'GRUNT ODDANY W UŻYTKOWANIE WIECZYSTE']

#------------------------------------------------------------------------------
### FUNKCJE ###################################################################
#------------------------------------------------------------------------------

def logger(id='', fn='', numer='', text='', level='INFO', info=None):
    """uzycie standardowego modulu `logging` zamiast wlasnej funkcji"""

    convert = {
        'ERROR':   logging.ERROR,
        'WARNING': logging.WARNING,
        'INFO':    logging.INFO,
        'DEBUG':   logging.DEBUG,
    }

    convert_color = {
        'ERROR':   C_RED,
        'WARNING': C_RED,
        'INFO':    C_GREEN,
        'DEBUG':   C_YELLOW,
    }
    if args.color:
        COLOR = convert_color[level]
    else:
        COLOR = ''

    if not id and info:
        id = info['numer_ksiegi_wieczystej'].replace('/', '-')
    if not fn and info:
        fn = info['nazwa_pliku'].keys()[0]

#    print('> id:',    repr(id))
#    print('> fn:',    repr(fn))
#    print('> numer:', repr(numer))
#    print('> text:',  repr(text))



    log.log(convert[level], '', extra={'type': level, 'id': id, 'fn': repr(fn), 'numer': numer, 'text': COLOR+repr(text)+C_RESET})

#------------------------------------------------------------------------------
### PARAMETRY W LINII KOMEND ##################################################
#------------------------------------------------------------------------------

parser = argparse.ArgumentParser()
parser.add_argument('-i', '--input', default='.',
                        help='Katalog z plikami wejsciowymi [domyslnie: input]')
parser.add_argument('-o', '--output', default='output/ksiegi_wieczyste.csv',
                        help='Plik wyjsciowy [domyslnie: output/ksiegi_wieczyste.csv]')
parser.add_argument('-l', '--logs', default='output/logs',
                        help='Katalog z logami [domyslnie: output/logs]')

parser.add_argument('-a', '--allow', action='store_true', default=False,
                        help='Pozwol na zapisywanie niekompletnych danych [domyslnie: NIE]')
                        
parser.add_argument('--wojewodztwa-csv', default='wojewodztwa.csv',
                        help='Plik CSV z kodami i przypisanymi wojewodztwami i nazwami plikow [domyslnie: wojewodztwa.csv]')
                        
parser.add_argument('-V', '--version', default=False, action='store_true', 
                        help='Numer wersji')

parser.add_argument('-D', '--debug', action='store_true',
                        help='Wypisywanie dodatkowych informacji [domyslnie: NIE]')
parser.add_argument('-DF', '--debug-folders', action='store_true',
                        help='Wypisywanie informacji o przegladanych katalogach [domyslnie: NIE]')
parser.add_argument('-C', '--color', action='store_true',
                        help='Stosowanie kolorow w logu [domyslnie: NIE]')
                        
args = parser.parse_args()

#------------------------------------------------------------------------------

if args.version:
    print('version: {}'.format(VERSION))
    exit(0)
    
#------------------------------------------------------------------------------

if args.debug:
    print('[DEBUG] args.input: {}'.format(args.input))
    print('[DEBUG] args.output: {}'.format(args.output))
    print('[DEBUG] args.log: {}'.format(args.logs))

#------------------------------------------------------------------------------


if args.color:
    C_RED    = '\033[1;31m'
    C_GREEN  = '\033[1;32m'
    C_YELLOW = '\033[1;33m'
    C_BLUE   = '\033[1;34m'
    C_PINK   = '\033[1;35m'
    C_RESET  = '\033[m'

else:
    C_RED    = ''
    C_GREEN  = ''
    C_YELLOW = ''
    C_BLUE   = ''
    C_PINK   = ''
    C_RESET  = ''

#------------------------------------------------------------------------------

folder_logs = args.logs
if not os.path.exists(folder_logs):
    os.makedirs(folder_logs)

log = logging.getLogger()

if args.debug:
    log.setLevel(logging.DEBUG)
else:
    log.setLevel(logging.INFO)

file_formatter = logging.Formatter(u'%(asctime)s;%(levelname)s;%(message)s;%(id)s;%(fn)s;%(numer)s;%(text)s', '%Y.%m.%d;%H:%M:%S')
file_handler = logging.FileHandler(datetime.datetime.now().strftime('{}/extracter__%Y%m%d__%H%M%S.csv'.format(folder_logs)), encoding='utf-8')
file_handler.setFormatter(file_formatter)
log.addHandler(file_handler)

console_formatter = logging.Formatter(u'%(asctime)s : %(levelname)s : %(message)s : %(id)s : %(fn)s : %(numer)s : %(text)s', '%Y.%m.%d : %H:%M:%S')
console_handler = logging.StreamHandler()
console_handler.setFormatter(console_formatter)
log.addHandler(console_handler)

if args.color:
    logging.addLevelName(logging.INFO,    "\033[1;32m{}\033[1;0m".format(logging.getLevelName(logging.INFO)))
    logging.addLevelName(logging.DEBUG,   "\033[1;33m{}\033[1;0m".format(logging.getLevelName(logging.DEBUG)))
    logging.addLevelName(logging.WARNING, "\033[1;31m{}\033[1;0m".format(logging.getLevelName(logging.WARNING)))
    logging.addLevelName(logging.ERROR,   "\033[1;41m{}\033[1;0m".format(logging.getLevelName(logging.ERROR)))

#------------------------------------------------------------------------------

if not os.path.exists('/'.join(args.output.split('/')[:-1])):
    os.makedirs('/'.join(args.output.split('/')[:-1]))

#------------------------------------------------------------------------------

if args.debug:
    logger(text='[DEBUG] args.input: {}'.format(args.input), level='DEBUG')
    logger(text='[DEBUG] args.output: {}'.format(args.output), level='DEBUG')
    logger(text='[DEBUG] args.logs: {}'.format(args.logs), level='DEBUG')

#------------------------------------------------------------------------------
# Wczytanie wojewodztw
#------------------------------------------------------------------------------

try:
    if sys.version_info[0] == 2:
        with open(args.wojewodztwa_csv, 'rb') as fh:
            reader = csv.reader(fh, delimiter=';')
            # loading the entire file
            wojewodztwa = [[item.decode('utf-8') for item in row] for row in reader]
            print(wojewodztwa)
    else:
        with open(args.wojewodztwa_csv, encoding='utf-8') as fh:
            reader = csv.reader(fh, delimiter=';')
            # wczytanie calego pliku
            wojewodztwa = list(reader)
        
    # pomijanie wierszy zawierajacych tylko jedna kolumne np. pustych lub zawierajacych komentarz
    wojewodztwa = [item for item in wojewodztwa if len(item) >= 2]

    # pomijanie wierszy zawierajace puste kolumny 
    wojewodztwa = {item[0]:item for item in wojewodztwa if item[0].strip() and item[1].strip() and item[2].strip()}
                
except IOError as ex:
    logger(text='ex: {}'.format(ex), level='ERROR')
    log.warning(ex)
    logger(text='KONIEC z powodu bledu przy czytaniu pliku z wojewodztwami', level='ERROR')
    if args.debug:
        traceback.print_exc(file=sys.stdout)
    exit(1)
    
#------------------------------------------------------------------------------
### FUNKCJE POMOCNICZE ########################################################
#------------------------------------------------------------------------------

def read_previous_ids(path):
    """
    Load only the 'id' column from an existing CSV.
    The file has about 81 columns, so loading only one makes
    that with a 9GB file it loads some 500MB of data 
    instead of 9GB
    """

    # czas start
    load_t_start = datetime.datetime.now()

    logger(text='[DEBUG] szukanie pliku: {}'.format(path), level='DEBUG')

    # wczytanie kolumny `id` z pliku do DF lub utworzenie pustego DF
    if os.path.isfile(path):
        logger(text='[DEBUG] tworzenie DF z pliku: {}'.format(path), level='DEBUG')
        ids = pd.read_csv(path, encoding='utf-8', sep=';', dtype=str, usecols=['id'])
    else:
        logger(text='[DEBUG] tworzenie nowego DF', level='DEBUG')
        ids = pd.DataFrame({'id': []})

    # deleting duplicate numbers
    ids_list = list(ids['id'].unique())

    logger(text='liczba wczytanych ID: {}'.format(len(ids)))
    logger(text='liczba unikalnych ID: {}'.format(len(ids_list)))

    #  czas stop
    load_t_stop = datetime.datetime.now()
    load_t_diff = load_t_stop - load_t_start
    logger(text='Czas wczytania CSV {} - wydajnosc {} minut na 100 000 ID ksiag'.format(load_t_diff, round( ((load_t_diff.total_seconds()/(len(ids) + 1))*100000/60), 1)))

    return ids_list

#------------------------------------------------------------------------------

def prepare_files_map(path):

    # both htm and html - subfolders are included
    lista_plikow = [{name: root} for root, dirs, files in os.walk(path) for name in files if name.lower().endswith((".html", ".htm"))]

    if args.debug_folders:
        for root, dirs, files in os.walk(path):
            for name in files:
                if name.lower().endswith((".html", ".htm")):
                    print(C_GREEN, 'TAK', '|', root, '|', name, C_RESET)
                else:
                    print(C_RED, 'NIE', '|', root, '|', name, C_RESET)

    # generating an ID based on the file name
    # all non-alphanumeric characters are converted to pauses (-)
    # replace multiple pauses with a single pause and delete NR as the prefix
    # assume format ID [A-Z0-9]-[0-9]-[0-9]
    slownik_nazw_plikow = {}

    for x in lista_plikow:
        # print(x)
        try:
            id_ = list(x.keys())[0]
            id_ = id_.upper()
            id_ = id_.replace('NR ', '').replace('.HTML', '').replace('.HTM', '')
            id_ = re.sub('[^a-zA-Z0-9]', '-', id_)
            id_ = re.sub('[-]+', '-', id_)
            id_ = re.sub('^-', '', id_)
            id_ = id_.split('-')
            if len(id_) > 3:
               id_ = id_[:3]
            else:
               id_ = id_[:2] # przypadek plikow LD1M-00018101-1.htm, LD1M-00018101-2.htm, LD1M-00018101-3.htm
            id_ = '-'.join(id_)
            logger(text='[DEBUG] prepare_files_map: ID: {} <- {}'.format(id_, list(x.keys())[0]), level='DEBUG')
            slownik_nazw_plikow.update({list(x.keys())[0]: {'id': id_, 'root': list(x.values())[0]}})
        except:
            logger('', list(x.keys())[0], '', u'nie mozna wczytac pliku ({})'.format(list(x.values())[0]), 'ERROR')

    # lista samych ID
    lista_id_plikow = list(set([x['id'] for x in slownik_nazw_plikow.values()]))

    # kazdemu identyfikatorowi przypisuje informacje o powiazanych z nim plikach
    ksiegi_wieczyste_mapa = {}

    for i in lista_id_plikow:
        ksiegi_wieczyste_mapa.update({i: {'pliki_powiazane': [{k: v['root']} for k, v in slownik_nazw_plikow.items() if v['id'] == i]}})

    return ksiegi_wieczyste_mapa

#------------------------------------------------------------------------------

def analyse_pliki_powiazane(pliki_powiazane, info=None):
    """
    Search for common elements in all files
    - typ pliku
    - numer ksiegi wieczystej
    - typ nieruchomosci
    """

    html_io = []
    html_ii = []
    html_iv = []

    for t in pliki_powiazane:
        filename = t.keys()[0]
        folder   = t.values()[0]
        with open(os.path.join(folder, filename), 'r', encoding='utf-8') as f:
            html = BeautifulSoup(f.read(), 'html.parser')

        # --- typ_pliku ---

        try:
            typ_pliku = html.select('.csTTytul')
            typ_pliku = typ_pliku[0].text if typ_pliku else None
        except Exception as ex:
            logger('', filename, '', u'nie mozna odczytac typu pliku z pliku ({})'.format(ex), 'ERROR', info=info)
            typ_pliku = None

        if typ_pliku not in [u'DZIAŁ I-O - OZNACZENIE NIERUCHOMOŚCI', # IO
                             u'DZIAŁ II - WŁASNOŚĆ',
                             u'DZIAŁ IV - HIPOTEKA',
                             u'OZNACZENIE KSIĘGI WIECZYSTEJ', # IO
                             ]:
            logger('', filename, '', u'plik zawiera informacje o dziale innym niz zdefiniowane - {}'.format(typ_pliku), 'WARNING', info=info)
            typ_pliku = None

        # --- numer_ksiegi_wieczystej ---

        try:
            numer_ksiegi_wieczystej = html.h2
            if numer_ksiegi_wieczystej:
                if numer_ksiegi_wieczystej.b:
                    # - schemat 1 -
                    numer_ksiegi_wieczystej = numer_ksiegi_wieczystej.b.text
                else:
                    # - schemat 2 -
                    numer_ksiegi_wieczystej = numer_ksiegi_wieczystej.text
                    numer_ksiegi_wieczystej = numer_ksiegi_wieczystej.split(',')[0].replace(u'TREŚĆ KSIĘGI WIECZYSTEJ NR', '').strip()

            if numer_ksiegi_wieczystej:
                numer_ksiegi_wieczystej = re.sub('[/]+', '/', re.sub('[^a-zA-Z0-9]', '/', numer_ksiegi_wieczystej.replace('NR ', '')))#.replace('/', '-')

            logger('', filename, '', 'numer_ksiegi_wieczystej: {}'.format(numer_ksiegi_wieczystej), level="DEBUG", info=info)

        except Exception as ex:
            logger('', filename, '', u'nie mozna odczytac numeru ksiegi wieczystej z pliku ({})'.format(ex), 'ERROR')
            numer_ksiegi_wieczystej = None

        # --- typ_nieruchomosci ---

        try:
            typ_nieruchomosci = html.h3
            if typ_nieruchomosci:
                # - schemat 1 -
                typ_nieruchomosci = typ_nieruchomosci.text
            else:
                # - schemat 2 -
                typ_nieruchomosci = find_tags_with_text(u'Typ księgi', html)
                if typ_nieruchomosci:
                    typ_nieruchomosci = typ_nieruchomosci[2].text
                else:
                    typ_nieruchomosci = find_tags_with_text(u'Sposób korzystania', html)
                    if typ_nieruchomosci:
                        if isinstance(typ_nieruchomosci[0], list):
                            typ_nieruchomosci = [x[-1].text for x in typ_nieruchomosci]
                            logger(text='wiele typow nieruchomosci - wybieram pierwszy: {}'.format(typ_nieruchomosci), level='WARNING', info=info)
                            typ_nieruchomosci = typ_nieruchomosci[0]
                        else:
                            typ_nieruchomosci = typ_nieruchomosci[-1].text
                    else:
                        typ_nieruchomosci = None
                        logger('', filename, '', u'nie mozna odczytac typu nieruchomosci z pliku (typ_nieruchomosci = None)', 'ERROR', info=info)

        except Exception as ex:
            logger('', filename, '', u'nie mozna odczytac typu nieruchomosci z pliku ({})'.format(ex), 'ERROR', info=info)
            typ_nieruchomosci = None

        # --- zebranie w calosc ---

        if typ_pliku and numer_ksiegi_wieczystej:# and typ_nieruchomosci: # nowy schemat nie ma typu nieruchomosci
            if typ_pliku == u'DZIAŁ I-O - OZNACZENIE NIERUCHOMOŚCI':
                html_io += [{'html': html, 'numer_ksiegi_wieczystej': numer_ksiegi_wieczystej,
                             'typ_nieruchomosci': typ_nieruchomosci, 'nazwa_pliku': t}]
            elif typ_pliku == u'DZIAŁ II - WŁASNOŚĆ':
                html_ii  = [{'html': html, 'numer_ksiegi_wieczystej': numer_ksiegi_wieczystej,
                             'typ_nieruchomosci': typ_nieruchomosci, 'nazwa_pliku': t}]
            elif typ_pliku == u'DZIAŁ IV - HIPOTEKA':
                html_iv  = [{'html': html, 'numer_ksiegi_wieczystej': numer_ksiegi_wieczystej,
                             'typ_nieruchomosci': typ_nieruchomosci, 'nazwa_pliku': t}]
            elif typ_pliku ==  u'OZNACZENIE KSIĘGI WIECZYSTEJ':
                html_io += [{'html': html, 'numer_ksiegi_wieczystej': numer_ksiegi_wieczystej,
                             'typ_nieruchomosci': typ_nieruchomosci, 'nazwa_pliku': t}]
        else:
            logger('', filename, '', 'brak typ_pliku ora numer_ksiegi_wieczystej', level='DEBUG')

    html_io_summary = {'numery_ksiag': [x['numer_ksiegi_wieczystej'] for x in html_io if x['numer_ksiegi_wieczystej']],
                       'typy_nieruchomosci': [x['typ_nieruchomosci'] for x in html_io if x['typ_nieruchomosci']]}
    html_ii_summary = {'numery_ksiag': [x['numer_ksiegi_wieczystej'] for x in html_ii if x['numer_ksiegi_wieczystej']],
                       'typy_nieruchomosci': [x['typ_nieruchomosci'] for x in html_ii if x['typ_nieruchomosci']]}
    html_iv_summary = {'numery_ksiag': [x['numer_ksiegi_wieczystej'] for x in html_iv if x['numer_ksiegi_wieczystej']],
                       'typy_nieruchomosci': [x['typ_nieruchomosci'] for x in html_iv if x['typ_nieruchomosci']]}

    return html_io, html_ii, html_iv, html_io_summary, html_ii_summary, html_iv_summary

#------------------------------------------------------------------------------

def find_tags_with_text(text, html):
    """
    Wyszukiwanie wierszy tabeli zawierajacych podany tekst

    Return:
        None          - gdy nic nie znaleziono
        wiersz        - gdy znaleziono tylko jeden wiersz
        lista wierszy - gdy znaleziono wiele wierszy
    """

    try:
        all_rows = [row.find_all('td') for row in html.find_all(lambda tag: tag.name == 'tr' and text in tag.text)]

        if len(all_rows) == 0:
            result = None        # zwrocenie None
        elif len(all_rows) == 1:
            result = all_rows[0] # zwrocenie pierwszego wiersza
        else:
            result = all_rows    # zwrocenie listy ze wszystkimi wierszami
    except Exception as ex:
        print('ex:', ex)
        result = None

    return result


def find_with_text(text, html, tag='tr', col=False):
    """
    Wyszukiwanie elementow zawierajacych podany tekst

    html: przeszukiwany html (BeautifulSoup)
    text: wyszukiwany tekst
    tag : przeszukiwany tag
    col : wyszukanie samego tekstu z podanej kolumny

    Return: lista znalezionych elementów lub pusta lista
    """

    try:
        result = [item for item in html.find_all(tag) if text in item.text]
        if result and col:
            result = [x.find_all('td')[col].text for x in result]
    except  Exception as ex:
        logger(text='find_with_text: exeption: {} {} {}'.format(ex, col, repr(result)), level='DEBUG')
        result = []

    return result

#------------------------------------------------------------------------------
# Dzielenie na rubryki i przeszukiwanie po rubrykach
#------------------------------------------------------------------------------

def dzielenie_na_rubryki(html=None, filename=None, split_text='ubryka'):

    #split='ubryka' = Rubryka, Podrubryka

    if filename:
        data = open(filename).read()
        html = BeautifulSoup(data, 'html.parser')

    all_trs = html.find_all('tr')

    all_rubryki = dict()

    rubryka_trs = []
    rubryka_nazwa = 'wstep'

    for tr in all_trs:
        if split_text in tr.text:
            #print(tr.text)
            if rubryka_nazwa not in all_rubryki:
                all_rubryki[rubryka_nazwa] = []
            all_rubryki[rubryka_nazwa].append(rubryka_trs)
            rubryka_trs = []
            rubryka_nazwa = tr.text
        if rubryka_trs is not None:
#            tr = [item.text.strip() for item in tr.find_all('td')] # pobranie samych tekstow, ale traci sie inne informacje
            if any(tr):
                rubryka_trs.append(tr)

    # dodanie ostatniego
    if rubryka_nazwa not in all_rubryki:
        all_rubryki[rubryka_nazwa] = []
    all_rubryki[rubryka_nazwa].append(rubryka_trs)

    return all_rubryki

def get_rubryka(all_rubryki, text):
    for key in all_rubryki.keys():
        if text in key:
            return all_rubryki[key]
    return None

def get_rows(rubryka, text):
    try:
        return [row for row in rubryka if text in row.text]
    except Exception as ex:
        logger(text='[DEBUG] {}'.format(ex), level='DEBUG')

#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
# DOKUMENT IO
#------------------------------------------------------------------------------

def extract_from_io(html, info=None):

    logger(text='[DEBUG] extract_from_io: poczatek', level='DEBUG', info=info)

    # --- schemat rubryka_0_1 ---

    logger(text='[DEBUG] extract_from_io: SCHEMAT 1 : rubryka_0_1: sprawdzanie', level='DEBUG', info=info)

    rubryka_0_1 = find_with_text(u'Rubryka 0.1 - Informacje podstawowe', html, 'table')

    if rubryka_0_1:
        result = extract_from_io_schemat_1(html, info)
        logger(text='[DEBUG] extract_from_io: koniec', level='DEBUG', info=info)
        return result

    # --- schemat rubryka_1_3 ---

    logger(text='[DEBUG] extract_from_io: SCHEMAT 2 : rubryka_1_3: sprawdzanie', level='DEBUG', info=info)

    rubryka_1_3 = find_with_text(u'Rubryka 1.3 - Położenie', html, 'table')

    if rubryka_1_3:
        result = extract_from_io_schemat_2(html, info)
        logger(text='[DEBUG] extract_from_io: koniec', level='DEBUG', info=info)
        return result

    # --- schemat lokal ----

    logger(text='[DEBUG] extract_from_io: SCHEMAT 3 : lokal: sprawdzanie', level='DEBUG', info=info)

    czy_lokal = find_tags_with_text(u'Lokal', html)

    if czy_lokal:
        result = extract_from_io_schemat_3(html, info)
        logger(text='[DEBUG] extract_from_io: koniec', level='DEBUG', info=info)
        return result

    # --- schemat sposob_korzystania ---

    logger(text='[DEBUG] extract_from_io: SCHEMAT 4 : sposob_korzystania: sprawdzanie', level='DEBUG', info=info)

    # u'Sposób korzystania' - moze byc wiecej niz jeden wiersz z tymi danymi
    sposob_korzystania = find_tags_with_text(u'Sposób korzystania', html)

    if sposob_korzystania:
        result = extract_from_io_schemat_4(html, info)
        logger(text='[DEBUG] extract_from_io: koniec', level='DEBUG', info=info)
        return result

    # --- brak ---

    logger(text='[DEBUG] extract_from_io: SCHEMAT brak', level='DEBUG', info=info)

    miejscowosc = None
    ulica = None
    numer_budynku = None
    numer_lokalu = None
    przeznaczenie_lokalu = None
    liczba_pokoi = None
    kondygnacja = None
    pole_powierzchni_uzytkowej= None

    logger(text='[DEBUG] extract_from_io: SCHEMAT: wynik {}'.format([miejscowosc, ulica, numer_budynku, numer_lokalu, przeznaczenie_lokalu, liczba_pokoi, kondygnacja, pole_powierzchni_uzytkowej]), level='DEBUG', info=info)

    logger(text='[DEBUG] extract_from_io: koniec', level='DEBUG', info=info)

    return miejscowosc, ulica, numer_budynku, numer_lokalu, przeznaczenie_lokalu, liczba_pokoi, kondygnacja, pole_powierzchni_uzytkowej

#------------------------------------------------------------------------------

def extract_from_io_schemat_1(html, info=None):
    """
    - przeznaczenie_lokalu = text
    - miejscowosc = None
    - ulica = None
    - numer_budynku = None
    - numer_lokalu = None
    - liczba_pokoi = None
    - kondygnacja = None
    - pole_powierzchni_uzytkowej = None
    """

    logger(text='[DEBUG] extract_from_io_schemat_1 : poczatek', level='DEBUG', info=info)

    rubryka_0_1 = find_with_text(u'Rubryka 0.1 - Informacje podstawowe', html, 'table')

    # inny sposob na numer ksiegi
    #numer_ksiegi = find_tags_with_text(u'Numer księgi', rubryka_0_1[0])
    #print('numer_ksiegi:', numer_ksiegi[-1].text.replace(' ', ''))

    logger(text='[DEBUG] extract_from_io_schemat_1 : przeznaczenie_lokalu', level='DEBUG', info=info)
    przeznaczenie_lokalu = find_tags_with_text(u'Typ księgi', rubryka_0_1[0])

    if przeznaczenie_lokalu:
        przeznaczenie_lokalu = przeznaczenie_lokalu[-1].text
    else:
        przeznaczenie_lokalu = None

    miejscowosc = None
    ulica = None
    numer_budynku = None
    numer_lokalu = None
    #przeznaczenie_lokalu = None
    liczba_pokoi = None
    kondygnacja = None
    pole_powierzchni_uzytkowej= None

    logger(text='[DEBUG] extract_from_io_schemat_1 : wynik {}'.format([miejscowosc, ulica, numer_budynku, numer_lokalu, przeznaczenie_lokalu, liczba_pokoi, kondygnacja, pole_powierzchni_uzytkowej]), level='DEBUG', info=info)

    logger(text='[DEBUG] extract_from_io_schemat_1 : koniec', level='DEBUG', info=info)

    return miejscowosc, ulica, numer_budynku, numer_lokalu, przeznaczenie_lokalu, liczba_pokoi, kondygnacja, pole_powierzchni_uzytkowej

#------------------------------------------------------------------------------

def extract_from_io_schemat_2(html, info=None):
    """
    - przeznaczenie_lokalu = text
    - miejscowosc = text
    - ulica = text
    - numer_budynku = None
    - numer_lokalu = None
    - liczba_pokoi = None
    - kondygnacja = None
    - pole_powierzchni_uzytkowej = None
    """

    logger(text='[DEBUG] extract_from_io_schemat_2 : poczatek', level='DEBUG', info=info)

    logger(text='[DEBUG] extract_from_io_schemat_2 : miejscowosc', level='DEBUG', info=info)
    rubryka_1_3 = find_with_text(u'Rubryka 1.3 - Położenie', html, 'table')
    #all_rubryki = dzielenie_na_rubryki(html)
    #rubryka_1_3 = get_rubryka(all_rubryki, u'Rubryka 1.3 - Położenie')
    #print(rubryka_1_3)
    miejscowosc = find_tags_with_text(u'Miejscowość', rubryka_1_3[0])[-1].text
    logger(text='[DEBUG] extract_from_io_schemat_2 : miejscowosc: {}'.format(repr(miejscowosc)), level='DEBUG', info=info)

    rubryka_1_4 = find_with_text(u'Rubryka 1.3 - Położenie', html, 'table')

    if rubryka_1_4:
        logger(text='[DEBUG] extract_from_io_schemat_2 : ulica', level='DEBUG', info=info)
        ulica = find_tags_with_text(u'Ulica', rubryka_1_4[0])

        logger(text='[DEBUG] extract_from_io_schemat_2 : przeznaczenie_lokalu', level='DEBUG', info=info)
        przeznaczenie_lokalu = find_tags_with_text(u'Ulica', rubryka_1_4[0])

        if not przeznaczenie_lokalu:
            logger(text='[DEBUG] extract_from_io_schemat_2 : sposob_korzystania', level='DEBUG', info=info)
            sposob_korzystania = find_tags_with_text(u'Sposób korzystania', html)

            if isinstance(sposob_korzystania[0], list) and sposob_korzystania[0]:
                sposob_korzystania = [x[-1].text for x in sposob_korzystania][0]
            else:
                sposob_korzystania = sposob_korzystania[-1].text

            logger(text=u'[DEBUG] extract_from_io_schemat_2 : sposob_korzystania: {}'.format(sposob_korzystania), level='DEBUG', info=info)
            przeznaczenie_lokalu = sposob_korzystania

    numer_budynku = None
    numer_lokalu = None
    liczba_pokoi = None
    kondygnacja = None
    pole_powierzchni_uzytkowej= None

    logger(text='[DEBUG] extract_from_io_schemat_2 : wynik {}'.format([miejscowosc, ulica, numer_budynku, numer_lokalu, przeznaczenie_lokalu, liczba_pokoi, kondygnacja, pole_powierzchni_uzytkowej]), level='DEBUG', info=info)

    logger(text='[DEBUG] extract_from_io_schemat_2 : koniec', level='DEBUG', info=info)

    return miejscowosc, ulica, numer_budynku, numer_lokalu, przeznaczenie_lokalu, liczba_pokoi, kondygnacja, pole_powierzchni_uzytkowej

#------------------------------------------------------------------------------

def extract_from_io_schemat_3(html, info=None):
    """
    - przeznaczenie_lokalu = text
    - miejscowosc = text
    - ulica = text
    - numer_budynku = text
    - numer_lokalu = text
    - liczba_pokoi = text
    - kondygnacja = text
    - pole_powierzchni_uzytkowej = text
    """

    logger(text='[DEBUG] extract_from_io_schemat_3 : poczatek', level='DEBUG', info=info)

    logger(text='[DEBUG] extract_from_io_schemat_3 : miejscowosc', level='DEBUG', info=info)
    miejscowosc = find_tags_with_text(u'Położenie', html)

    if miejscowosc:
        if not isinstance(miejscowosc[0], list):
            miejscowosc = miejscowosc[3].text.split(', ') if len(miejscowosc) > 3 else [None]
            miejscowosc = miejscowosc[0] if len(miejscowosc) <= 3 else miejscowosc[3]
        else:
            ilosc = len(miejscowosc)
            miejscowosc = [x[-1].text.split(', ') for x in miejscowosc][0]
            print(miejscowosc)
            logger(text=u'extract_from_io_schemat_3 : miejscowosc : wiele miejscowosci ({}) wybieram pierwsza: {}'.format(ilosc, miejscowosc), level='WARNING', info=info)


    logger(text='[DEBUG] extract_from_io_schemat_3 : ulica', level='DEBUG', info=info)
    ulica = find_tags_with_text(u'Ulica', html)

    if ulica:
        if not isinstance(ulica[0], list):
            ulica = ulica[3].text
        else:
            ilosc = len(ulica)
            ulica = [x[-1].text.split(', ') for x in ulica][0]
            logger(text=u'extract_from_io_schemat_3 : ulica : wiele ulic ({}) wybieram pierwsza: {}'.format(ilosc, ulica), level='WARNING', info=info)
    else:
         None

    logger(text='[DEBUG] extract_from_io_schemat_3 : numer_budynku', level='DEBUG', info=info)
    numer_budynku = find_tags_with_text(u'Numer budynku', html)
    numer_budynku = numer_budynku[4].text if numer_budynku else None

    logger(text='[DEBUG] extract_from_io_schemat_3 : numer_lokalu', level='DEBUG', info=info)
    numer_lokalu = find_tags_with_text(u'Numer lokalu', html)
    numer_lokalu = numer_lokalu[5].text if numer_lokalu else None

    logger(text='[DEBUG] extract_from_io_schemat_3 : przeznaczenie_lokalu', level='DEBUG', info=info)
    przeznaczenie_lokalu = find_tags_with_text(u'Przeznaczenie lokalu', html)
    przeznaczenie_lokalu = przeznaczenie_lokalu[1].text if przeznaczenie_lokalu else None

    logger(text='[DEBUG] extract_from_io_schemat_3 : liczba_pokoi', level='DEBUG', info=info)
    liczba_pokoi = find_tags_with_text(u'Opis lokalu', html)

    if liczba_pokoi:
        liczba_pokoi = liczba_pokoi[1].text.replace('\r\n', '').replace('\n', ' ').split(', ')
        liczba_pokoi = [x for x in liczba_pokoi if x.find(u'POKÓJ') > -1]
        # DODANE: zamiana ilosci pokoi na tekst aby nie zapisywalo jako float 0.0, 1.0, itp.
        liczba_pokoi = str(sum([int(x.split(' - ')[-1]) if x.find(' - ') > -1 else 1 for x in liczba_pokoi]))  # jak nie ma znaku - to - licze jako 1
    else:
        liczba_pokoi = None

    logger(text='[DEBUG] extract_from_io_schemat_3 : kondygnacja', level='DEBUG', info=info)
    kondygnacja = find_tags_with_text(u'Kondygnacja', html)
    kondygnacja = kondygnacja[1].text if kondygnacja else None

    logger(text='[DEBUG] extract_from_io_schemat_3 : pole_powierzchni_uzytkowej', level='DEBUG', info=info)
    pole_powierzchni_uzytkowej = find_tags_with_text(u'Pole powierzchni', html)
    pole_powierzchni_uzytkowej = pole_powierzchni_uzytkowej[1].text if pole_powierzchni_uzytkowej else None

    logger(text='[DEBUG] extract_from_io_schemat_3 : wynik {}'.format([miejscowosc, ulica, numer_budynku, numer_lokalu, przeznaczenie_lokalu, liczba_pokoi, kondygnacja, pole_powierzchni_uzytkowej]), level='DEBUG', info=info)

    logger(text='[DEBUG] extract_from_io_schemat_3 : koniec', level='DEBUG', info=info)

    return miejscowosc, ulica, numer_budynku, numer_lokalu, przeznaczenie_lokalu, liczba_pokoi, kondygnacja, pole_powierzchni_uzytkowej

#------------------------------------------------------------------------------

def extract_from_io_schemat_4(html, info=None):
    """
    - przeznaczenie_lokalu = text lub None
    - miejscowosc = text
    - ulica = text lub None
    - numer_budynku = None
    - numer_lokalu = None
    - liczba_pokoi = None
    - kondygnacja = None
    - pole_powierzchni_uzytkowej = None
    """

    logger(text='[DEBUG] extract_from_io_schemat_4 : poczatek', level='DEBUG', info=info)

    logger(text='[DEBUG] extract_from_io_schemat_4 : sposob_korzystania', level='DEBUG', info=info)
    sposob_korzystania = find_tags_with_text(u'Sposób korzystania', html)

    if isinstance(sposob_korzystania[0], list) and sposob_korzystania[0]:
        sposob_korzystania = [x[1].text for x in sposob_korzystania][0]
    else:
        sposob_korzystania = sposob_korzystania[1].text

    logger(text=u'[DEBUG] extract_from_io_schemat_4 : sposob_korzystania: {}'.format(sposob_korzystania), level='DEBUG', info=info)

    przeznaczenie_lokalu = sposob_korzystania

    #---

    logger(text='[DEBUG] extract_from_io_schemat_4 : miejscowosc', level='DEBUG', info=info)
    miejscowosc = find_tags_with_text(u'Położenie', html)

    if miejscowosc:
        if isinstance(miejscowosc, bs4.element.ResultSet):
            miejscowosc = miejscowosc[3].text.split(', ') if len(miejscowosc) > 3 else [None]
            miejscowosc = miejscowosc[0] if len(miejscowosc) <= 3 else miejscowosc[3]
        else:
            ilosc = len(miejscowosc)
            miejscowosc = [x[3].text.split(', ') for x in miejscowosc]
            logger(text=u'extract_from_io_schemat_4 : miejscowosc : wiele miejscowosci ({}) wybieram pierwsza: {} z {}'.format(ilosc, miejscowosc[0], miejscowosc), level='WARNING', info=info)
            miejscowosc = miejscowosc[0]
    #---

    logger(text='[DEBUG] extract_from_io_schemat_4 : ulica', level='DEBUG', info=info)
    ulica = find_tags_with_text(u'Ulica', html)

    if ulica:
        if isinstance(ulica, bs4.element.ResultSet):
            ulica = ulica[1].text
        elif isinstance(ulica, list) and isinstance(ulica[0], bs4.element.ResultSet):
            ilosc = len(ulica)
            ulica = [x[1].text for x in ulica]
            logger(text=u'extract_from_io_schemat_4 : ulica : wiele ulic ({}) wybieram pierwsza: {} z {}'.format(ilosc, ulica[0], ulica), level='WARNING', info=info)
            ulica = ulica[0]

    #---

    numer_budynku = None
    numer_lokalu = None
    liczba_pokoi = None
    kondygnacja = None
    pole_powierzchni_uzytkowej = None

    #---

    logger(text='[DEBUG] extract_from_io_schemat_4 : wynik {}'.format([miejscowosc, ulica, numer_budynku, numer_lokalu, przeznaczenie_lokalu, liczba_pokoi, kondygnacja, pole_powierzchni_uzytkowej]), level='DEBUG', info=info)

    logger(text='[DEBUG] extract_from_io_schemat_4 : koniec', level='DEBUG', info=info)

    return miejscowosc, ulica, numer_budynku, numer_lokalu, przeznaczenie_lokalu, liczba_pokoi, kondygnacja, pole_powierzchni_uzytkowej

#------------------------------------------------------------------------------

def pesel_to_data_urodzin(pesel):
    data_urodzenia = (
       pesel[4:6] + '-' + pesel[2:4] + '-19' + pesel[:2] if int(pesel[2:4]) < 20 else pesel[4:6] + '-' + str(
       int(pesel[2:4]) - 20) + '-20' + pesel[:2]) if pesel else None
    return data_urodzenia

def extract_osoba_fizyczna(osoba_fizyczna, info=None):
    """
    imie1 = text lub None
    imie2 = = text lub None
    nazwisko = = text lub None
    ojciec = = text lub None
    matka = = text lub None
    pesel = = text lub None
    data_urodzenia = = text lub None
    """
    logger(text='[DEBUG] extract_osoba_fizyczna v1: poczatek: {}'.format(osoba_fizyczna), level='DEBUG', info=info)

    if osoba_fizyczna:
        imie_nazwisko = osoba_fizyczna[0].split(' ')

        imie1 = imie_nazwisko[0]
        imie2 = imie_nazwisko[1] if len(imie_nazwisko) > 1 else None

        nazwisko = imie_nazwisko[-1]

        if imie1 == imie2 or imie2 == nazwisko:
            imie2 = None
        if len(osoba_fizyczna) > 2:
            ojciec = osoba_fizyczna[1]
            matka = osoba_fizyczna[2]
            matka = re.findall('^[^\d]+', matka)[0] if re.findall('[0-9]{11}', matka) else matka # gdy brak przecinka miedzy matka a PESEL - 'MARIA70041004109'
        else:
            ojciec = None
            matka = None

        pesel = [re.findall('[0-9]{11}', x)[0] for x in osoba_fizyczna if re.findall('[0-9]{11}', x)] # gdy brak przecinka miedzy matka a PESEL - 'MARIA70041004109'
        pesel = pesel[0] if pesel else None
        if pesel:
            data_urodzenia = pesel_to_data_urodzin(pesel)
        else:
            data_urodzenia = None
    else:
        imie1 = None
        imie2 = None
        nazwisko = None
        ojciec = None
        matka = None
        pesel = None
        data_urodzenia = None

    return {
        'os_imie1': imie1,
        'os_imie2': imie2,
        'os_nazwisko': nazwisko,
        'os_ojciec': ojciec,
        'os_matka': matka,
        'os_pesel': pesel,
        'os_data_urodzenia': data_urodzenia,
        'os_numer_udzialu_w_prawie': '',
        'os_rodzaj_wspolnosci': '',
    }

#------------------------------------------------------------------------------

def extract_osoba_fizyczna_v2(html, info=None):

    logger(text='[DEBUG] extract osoba fizyczna v2: poczatek: HTML', level='DEBUG', info=info)

    imie1 = None
    imie2 = None
    nazwisko = None
    ojciec = None
    matka = None
    pesel = None
    data_urodzenia = None

    try:
        table = find_with_text(u'Osoba fizyczna', html, 'table')[0]

        all_udzial = find_tags_with_text(u'Lista wskazań udziałów w prawie', table)
        if all_udzial:
            if isinstance(all_udzial[0], list):
                all_udzial = [item[-1].get_text(strip=True).replace('---', '') for item in all_udzial]
            else:
                all_udzial = [all_udzial[-1].get_text(strip=True).replace('---', '')]

        all_imie1 = find_tags_with_text(u'Imię pierwsze', table)
        if all_imie1:
            if isinstance(all_imie1[0], list):
                all_imie1 = [item[-1].get_text(strip=True).replace('---', '') for item in all_imie1]
            else:
                all_imie1 = [all_imie1[-1].get_text(strip=True).replace('---', '')]

        all_imie2 = find_tags_with_text(u'Imię drugie', table)
        if all_imie2:
            if isinstance(all_imie2[0], list):
                all_imie2 = [item[-1].get_text(strip=True).replace('---', '') for item in all_imie2]
            else:
                all_imie2 = [all_imie2[-1].get_text(strip=True).replace('---', '')]

        all_nazwisko1 = find_tags_with_text(u'Nazwisko / pierwszy człon nazwiska złożonego', table)
        if all_nazwisko1:
            if isinstance(all_nazwisko1[0], list):
                all_nazwisko1 = [item[-1].get_text(strip=True).replace('---', '') for item in all_nazwisko1]
            else:
                all_nazwisko1 = [all_nazwisko1[-1].get_text(strip=True).replace('---', '')]

        all_nazwisko2 = find_tags_with_text(u'Drugi człon nazwiska złożonego', table)
        if all_nazwisko2:
            if isinstance(all_nazwisko2[0], list):
                all_nazwisko2 = [item[-1].get_text(strip=True).replace('---', '') for item in all_nazwisko2]
            else:
                all_nazwisko2 = [all_nazwisko2[-1].get_text(strip=True).replace('---', '')]

        all_ojciec = find_tags_with_text(u'Imię ojca', table)
        if all_ojciec:
            if isinstance(all_ojciec[0], list):
                all_ojciec = [item[-1].get_text(strip=True).replace('---', '') for item in all_ojciec]
            else:
                all_ojciec = [all_ojciec[-1].get_text(strip=True).replace('---', '')]

        all_matka = find_tags_with_text(u'Imię matki', table)
        if all_matka:
            if isinstance(all_matka[0], list):
                all_matka = [item[-1].get_text(strip=True).replace('---', '') for item in all_matka]
            else:
                all_matka = [all_matka[-1].get_text(strip=True).replace('---', '')]

        all_pesel = find_tags_with_text(u'PESEL', table)
        if all_pesel:
            if isinstance(all_pesel[0], list):
                all_pesel = [item[-1].get_text(strip=True).replace('---', '') for item in all_pesel]
            else:
                all_pesel = [all_pesel[-1].get_text(strip=True).replace('---', '')]

        wszystkie_osoby_fizyczne = []

        if not all([all_udzial, all_imie1, all_imie2, all_nazwisko1, all_nazwisko2, all_ojciec, all_matka, all_pesel]):
            logger(text='Brak osoby fizycznej', level='INFO', info=info)
            return []

        for udzial, imie1, imie2, nazwisko1, nazwisko2, ojciec, matka, pesel  in zip(all_udzial, all_imie1, all_imie2, all_nazwisko1, all_nazwisko2, all_ojciec, all_matka, all_pesel):
            nazwisko = nazwisko1
            if nazwisko2:
                nazwisko += '-' + nazwisko2

            data_urodzenia = pesel_to_data_urodzin(pesel)

            wszystkie_osoby_fizyczne.append({
                'os_imie1': imie1,
                'os_imie2': imie2,
                'os_nazwisko': nazwisko,
                'os_ojciec': ojciec,
                'os_matka': matka,
                'os_pesel': pesel,
                'os_data_urodzenia': data_urodzenia,
                'os_numer_udzialu_w_prawie': udzial,
                'os_rodzaj_wspolnosci': '',
            })
            if args.debug:
                print('[DEBUG] Lista wskazań udziałów w prawie:', udzial)
                print('[DEBUG] Imię pierwsze:', imie1)
                print('[DEBUG] Imię drugie:', imie2)
                print('[DEBUG] Nazwisko / pierwszy człon nazwiska złożonego:', nazwisko1)
                print('[DEBUG] Drugi człon nazwiska złożonego:', nazwisko2)
                print('[DEBUG] -- nazwisko:', nazwisko)
                print('[DEBUG] Imię ojca:', ojciec)
                print('[DEBUG] Imię matki:', matka)
                print('[DEBUG] PESEL:', pesel)
                print('[DEBUG] Data urodzenia:', data_urodzenia)
                print('[DEBUG] Rodzaj wspolnosci:', '')
                print('----------------')
    except Exception as ex:
        wszystkie_osoby_fizyczne = []
        print('ex:', ex)

    return wszystkie_osoby_fizyczne

#------------------------------------------------------------------------------

def extract_podmiot(podmiot, info=None):

    logger(text='[DEBUG] extract_podmiot: poczatek: {}'.format(podmiot), level='DEBUG', info=info)

    if podmiot:
        nazwa_podmiotu = podmiot[0]
        miejscowosc_podmiotu = podmiot[1] if len(podmiot) > 1 else None
        regon = podmiot[2] if len(podmiot) > 2 else None
    else:
        nazwa_podmiotu = None
        miejscowosc_podmiotu = None
        regon = None

    return {
        'p_nazwa_podmiotu': nazwa_podmiotu,
        'p_miejscowosc_podmiotu': miejscowosc_podmiotu,
        'p_regon': regon,
    }

#------------------------------------------------------------------------------
# DOKUMENT II
#------------------------------------------------------------------------------

def extract_from_ii(html, info=None):

    logger(text='[DEBUG] extract_from_ii: poczatek: HTML', level='DEBUG', info=info)

    schemat = find_tags_with_text('Rubryka', html)

    if schemat:
        logger(text='[DEBUG] extract_from_ii: SCHEMAT 2: wybrany', level='DEBUG', info=info)
        return extract_from_ii_schemat_2(html, info)
    else:
        logger(text='[DEBUG] extract_from_ii: SCHEMAT 1: wybrany', level='DEBUG', info=info)
        return extract_from_ii_schemat_1(html, info)

#------------------------------------------------------------------------------

def extract_from_ii_schemat_1(html, info=None):
    """
    osoby_fizyczne = [osoba, ... ]
        osoba = {'os_imie1': imie1, 'os_imie2': imie2, 'os_nazwisko': nazwisko,
                'os_ojciec': ojciec, 'os_matka': matka,
                'os_pesel': pesel, 'os_data_urodzenia': data_urodzenia,
                'os_numer_udzialu_w_prawie': udzial, 'os_rodzaj_wspolnosci': '',
    rodzaj_wspolnosci = lista
    numer_udzialu_w_prawie = [numer, ... ]
    rozdzielnosc_majatkowa = lista
    podmioty =  [podmiot1, podmiot2, podmiot3]
        podmiot1 = {'p_nazwa_podmiotu': nazwa_podmiotu, 'p_miejscowosc_podmiotu': miejscowosc_podmiotu, 'p_regon': regon}
        podmiot2 = {'p_nazwa_podmiotu': nazwa_podmiotu, 'p_miejscowosc_podmiotu': miejscowosc_podmiotu, 'p_regon': regon}
        podmiot3 = {'p_nazwa_podmiotu': nazwa_podmiotu, 'p_miejscowosc_podmiotu': miejscowosc_podmiotu, 'p_regon': regon}
    """

    rodzaj_wspolnosci = None
    numer_udzialu_w_prawie = []


    logger(text='[DEBUG] extract_from_ii: SCHEMAT 1: poczatek: HTML', level='DEBUG', info=info)

    # --- osoby fizyczne ---

    logger(text='[DEBUG] extract_from_ii: SCHEMAT 1: osoba_fizyczna: sprawdzanie', level='DEBUG', info=info)

    table_osoby_fizyczne = find_with_text(u'Osoba fizyczna', html, tag='table')
    if table_osoby_fizyczne:
        logger(text='[DEBUG] extract_from_ii: SCHEMAT 1: table_osoba_fizyczna: {}'.format(len(table_osoby_fizyczne)), level='DEBUG', info=info)

        osoby_fizyczne = find_tags_with_text(u'Osoba fizyczna', table_osoby_fizyczne[0])

        logger(text='[DEBUG] extract_from_ii: SCHEMAT 1: osoba_fizyczna: wybrane', level='DEBUG', info=info)


        if not isinstance(osoby_fizyczne[0], list):
            osoby_fizyczne = [osoby_fizyczne]

        osoby_fizyczne = [extract_osoba_fizyczna(x[1].text.split(', '), info)
                            for x in osoby_fizyczne]

        # ---

        rodzaj_wspolnosci = find_tags_with_text(u'Lista wskazań udziałów w prawie', table_osoby_fizyczne[0])

        if not isinstance(rodzaj_wspolnosci[0], list):
            rodzaj_wspolnosci = [rodzaj_wspolnosci]

        # DODANE: numer udzialu w prawie aby laczyc z hipotekami
        numer_udzialu_w_prawie = [(x[2].text if len(x) > 2 else []) for x in rodzaj_wspolnosci]
        #logger(text='[DEBUG]] extract_from_ii: SCHEMAT 1: osoba numer_udzialu_w_prawie: {}'.format(len(numer_udzialu_w_prawie)), level="DEBUG")
        #logger(text='[DEBUG]] extract_from_ii: SCHEMAT 1: osoba numer_udzialu_w_prawie: {}'.format(numer_udzialu_w_prawie), level="DEBUG")

        rodzaj_wspolnosci = [x[4].text.replace('---', '-') if len(x) > 4 else [] for x in rodzaj_wspolnosci]

        logger(text='[DEBUG] extract_from_ii: SCHEMAT 1: [osoby_fizycznem, rodzaj wspolnosci, numer_udzialu_w_prawie]: {}'.format([len(osoby_fizyczne), len(rodzaj_wspolnosci), len(numer_udzialu_w_prawie)]), level="DEBUG", info=info)

        for x, y, z in zip(osoby_fizyczne, rodzaj_wspolnosci, numer_udzialu_w_prawie):
            x['os_rodzaj_wspolnosci'] = y
            x['os_numer_udzialu_w_prawie'] = z

        #print(osoby_fizyczne)

        # ---

        rozdzielnosc_majatkowa = find_tags_with_text(u'ROZDZIELNOŚĆ MAJĄTKOWA', table_osoby_fizyczne[0])
        #print(len(rozdzielnosc_majatkowa))
        rozdzielnosc_majatkowa = 'TAK' if rozdzielnosc_majatkowa else None

        # ---

        # DODANE: usuwanie powtarzajacych sie (osoba_fizyczna, rodzaj wspolnosci)
        #if osoby_fizyczne and rodzaj_wspolnosci and numer_udzialu_w_prawie:
        #    result = []
        #    for pair in zip(osoby_fizyczne, rodzaj_wspolnosci, numer_udzialu_w_prawie):
        #        if pair not in result:
        #            result.append(pair)
        #    osoby_fizyczne, rodzaj_wspolnosci = list(zip(*result))

    else:
        logger(text='[DEBUG] extract_from_ii: SCHEMAT 1: osoba_fizyczna: brak', level='DEBUG', info=info)

        osoby_fizyczne = []
        rozdzielnosc_majatkowa = None

    # --- podmioty - inna_osoba_prawna ---

    logger(text='[DEBUG] extract_from_ii: SCHEMAT 1: inna_osoba_prawna: sprawdzanie', level='DEBUG', info=info)

    # domyslne wartosci gdy brak danych
    podmioty_1 = []
    podmioty_1_rodzaj_wspolnosci = None
    podmioty_1_numer_udzialu_w_prawie = []

    table_podmiot_1 = find_with_text(u'Inna osoba prawna', html, tag='table')

    if table_podmiot_1:
        podmioty_1 = find_tags_with_text(u'Inna osoba prawna', table_podmiot_1[0])

        logger(text='[DEBUG] extract_from_ii: SCHEMAT 1: inna_osoba_prawna: wybrane', level='DEBUG', info=info)

        if podmioty_1 and not isinstance(podmioty_1[0], list):
            podmioty_1 = [podmioty_1]
        podmioty_1 = [extract_podmiot(x[1].text.split(', '), info) for x in podmioty_1]

        podmioty_1_rodzaj_wspolnosci = find_tags_with_text(u'Lista wskazań udziałów w prawie', table_podmiot_1[0])

        if not isinstance(podmioty_1_rodzaj_wspolnosci[0], list):
            podmioty_1_rodzaj_wspolnosci = [podmioty_1_rodzaj_wspolnosci]

        # DODANE: numer udzialu w prawie aby laczyc z hipotekami
        podmioty_1_numer_udzialu_w_prawie = [(x[2].text if len(x) > 2 else []) for x in podmioty_1_rodzaj_wspolnosci]

        for x, y, z in zip(podmioty_1, podmioty_1_rodzaj_wspolnosci, podmioty_1_numer_udzialu_w_prawie):
            x['rodzaj_wspolnosci'] = y
            x['numer_udzialu_w_prawie'] = z

    else:
        podmioty_1 = []
        podmioty_1_rodzaj_wspolnosci = None
        podmioty_1_numer_udzialu_w_prawie = []

    # --- podmioty - jednostka_samorzadowa ---

    logger(text='[DEBUG] extract_from_ii: SCHEMAT 1: jednostka_samorzadowa: sprawdzanie', level='DEBUG', info=info)

    # domyslne wartosci gdy brak danych
    podmioty_2 = []
    podmioty_2_rodzaj_wspolnosci = None
    podmioty_2_numer_udzialu_w_prawie = []

    podmioty_2 = find_tags_with_text(u'Jednostka samorządu', html)

    if podmioty_2:
        logger(text='[DEBUG] extract_from_ii: SCHEMAT 1: jednostka_samorzadowa: wybrane', level='DEBUG', info=info)

        # schemat 1
        logger(text='[DEBUG] extract_from_ii: SCHEMAT 1: jednostka_samorzadowa: schemat 1 wybrane', level='DEBUG', info=info)

        if not isinstance(podmioty_2[0], list):
            podmioty_2 = [podmioty_2]
        podmioty_2 = [extract_podmiot(x[1].text.split(', '), info) for x in podmioty_2]
    else:
        podmioty_2 = []
        podmioty_2_rodzaj_wspolnosci = None
        podmioty_2_numer_udzialu_w_prawie = []

    # --- podmioty - skarb_panstwa ---

    logger(text='[DEBUG] extract_from_ii: SCHEMAT 1: skarb_panstwa: sprawdzanie', level='DEBUG', info=info)

    # domyslne wartosci gdy brak danych
    podmioty_3 = []
    podmioty_3_rodzaj_wspolnosci = None
    podmioty_3_numer_udzialu_w_prawie = []

    podmioty_3 = find_tags_with_text(u'Skarb Państwa', html)

    if podmioty_3:

        logger(text='[DEBUG] extract_from_ii: SCHEMAT 1: skarb_panstwa: wybrane', level='DEBUG', info=info)

        # schemat 1
        logger(text='[DEBUG] extract_from_ii: SCHEMAT 1: skarb_panstwa: schemat 1 wybrane', level='DEBUG', info=info)

        if not isinstance(podmioty_3[0], list):
            podmioty_3 = [podmioty_3]
        podmioty_3 = [extract_podmiot(x[1].text.split(', '), info) for x in podmioty_3]
    else:
        podmioty_3 = []
        podmioty_3_rodzaj_wspolnosci = None
        podmioty_3_numer_udzialu_w_prawie = []

    podmioty = podmioty_1 + podmioty_2 + podmioty_3

    numer_udzialu_w_prawie = numer_udzialu_w_prawie + podmioty_1_numer_udzialu_w_prawie + podmioty_2_numer_udzialu_w_prawie + podmioty_3_numer_udzialu_w_prawie

    logger(text='[DEBUG] extract_from_ii: SCHEMAT 1: koniec', level='DEBUG', info=info)

    return osoby_fizyczne, rodzaj_wspolnosci, numer_udzialu_w_prawie, rozdzielnosc_majatkowa, podmioty

#------------------------------------------------------------------------------

def extract_from_ii_schemat_2(html, info=None):
    """
    osoby_fizyczne = [osoba, ... ]
        osoba = {'os_imie1': imie1, 'os_imie2': imie2, 'os_nazwisko': nazwisko,
                'os_ojciec': ojciec, 'os_matka': matka,
                'os_pesel': pesel, 'os_data_urodzenia': data_urodzenia,
                'os_numer_udzialu_w_prawie': udzial, 'os_rodzaj_wspolnosci': '',
    rodzaj_wspolnosci = lista
    numer_udzialu_w_prawie = lista
    rozdzielnosc_majatkowa = lista
    podmioty = [podmiot1, podmiot2, podmiot3]
        podmiot1 = {'p_nazwa_podmiotu': nazwa_podmiotu, 'p_miejscowosc_podmiotu': miejscowosc_podmiotu, 'p_regon': regon}
        podmiot2 = {'p_nazwa_podmiotu': nazwa_podmiotu, 'p_miejscowosc_podmiotu': miejscowosc_podmiotu, 'p_regon': regon}
        podmiot3 = {'p_nazwa_podmiotu': nazwa_podmiotu, 'p_miejscowosc_podmiotu': miejscowosc_podmiotu, 'p_regon': regon}
    """

    all_rubryki = dzielenie_na_rubryki(html)
    #print(all_rubryki.keys())


    logger(text='[DEBUG] extract_from_ii: SCHEMAT 2: poczatek: HTML', level='DEBUG', info=info)

    # list z indeskami i dostepnymi wartosciami
    temp_numer_udzialu_w_prawie = find_tags_with_text(u'Numer udziału w prawie', html)
    if temp_numer_udzialu_w_prawie and not isinstance(temp_numer_udzialu_w_prawie[0], list):
        temp_numer_udzialu_w_prawie = [temp_numer_udzialu_w_prawie]
    temp_numer_udzialu_w_prawie = [x[-1].text for x in temp_numer_udzialu_w_prawie]
    logger(text='[DEBUG] extract_from_ii: SCHEMAT 2: temp_numer_udzialu_w_prawie: {}'.format(temp_numer_udzialu_w_prawie), level='DEBUG', info=info)

    # list z indeskami i dostepnymi wartosciami
    temp_rodzaj_wspolnosci = find_tags_with_text(u'Rodzaj wspólności', html)
    if temp_rodzaj_wspolnosci and not isinstance(temp_rodzaj_wspolnosci[0], list):
        temp_rodzaj_wspolnosci = [temp_rodzaj_wspolnosci]
    temp_rodzaj_wspolnosci = [x[-1].text for x in temp_rodzaj_wspolnosci]
    temp_rodzaj_wspolnosci = [x.replace(u'-', u'') for x in temp_rodzaj_wspolnosci]
    logger(text='[DEBUG] extract_from_ii: SCHEMAT 2: temp_rodzaj_wspolnosci: {}'.format(temp_rodzaj_wspolnosci), level='DEBUG', info=info)


    rubryka_osoba_fizyczna = get_rubryka(all_rubryki, u'Osoba fizyczna')

    # indexy do list _numer_udzialu_w_prawie i _rodzaj_wspolnosci
    lista_wskazan_udzialow_w_prawie = get_rows(rubryka_osoba_fizyczna[0], u'Lista wskazań udziałów w prawie')
    lista_wskazan_udzialow_w_prawie = [x.find_all('td') for x in lista_wskazan_udzialow_w_prawie]

    if lista_wskazan_udzialow_w_prawie and not isinstance(lista_wskazan_udzialow_w_prawie[0], list):
        lista_wskazan_udzialow_w_prawie = [lista_wskazan_udzialow_w_prawie]
    lista_wskazan_udzialow_w_prawie = [x[-1].text for x in lista_wskazan_udzialow_w_prawie]
    #print(lista_wskazan_udzialow_w_prawie)

    logger(text='[DEBUG] extract_from_ii: SCHEMAT 2: lista_wskazan_udzialow_w_prawie: {}'.format(lista_wskazan_udzialow_w_prawie), level='DEBUG', info=info)

    lista_wskazan_udzialow_w_prawie = [int(item) for item in lista_wskazan_udzialow_w_prawie]

    numer_udzialu_w_prawie = [temp_numer_udzialu_w_prawie[index-1] for index in lista_wskazan_udzialow_w_prawie]
    rodzaj_wspolnosci = [temp_rodzaj_wspolnosci[index-1] for index in lista_wskazan_udzialow_w_prawie]

    logger(text='[DEBUG] extract_from_ii: SCHEMAT 2: numer_udzialu_w_prawie: {}'.format(numer_udzialu_w_prawie), level='DEBUG', info=info)
    logger(text='[DEBUG] extract_from_ii: SCHEMAT 2: rodzaj_wspolnosci: {}'.format(rodzaj_wspolnosci), level='DEBUG', info=info)


    # --- osoby fizyczne ---

#    logger(text='[DEBUG] extract_from_ii: osoba_fizyczna: schemat 2 sprawdzanie', level='DEBUG', info=info)

    osoby_fizyczne = find_tags_with_text(u'Osoba fizyczna', html)
    logger(text='[DEBUG] osoby_fizyczne: przed: {}'.format(osoby_fizyczne), level='DEBUG', info=info)

    if osoby_fizyczne:

        logger(text='[DEBUG] extract_from_ii: SCHEMAT 2: osoba_fizyczna: wybrane', level='DEBUG', info=info)

        osoby_fizyczne = extract_osoba_fizyczna_v2(html, info)
        logger(text='[DEBUG] osoby_fizyczne: po: {}'.format(osoby_fizyczne), level='DEBUG', info=info)

        for x, y, z in zip(osoby_fizyczne, rodzaj_wspolnosci, numer_udzialu_w_prawie):
            x['os_rodzaj_wspolnosci'] = y
            x['os_numer_udzialu_w_prawie'] = z

        # ---

        rozdzielnosc_majatkowa = find_tags_with_text(u'ROZDZIELNOŚĆ MAJĄTKOWA', html)
        rozdzielnosc_majatkowa = 'TAK' if rozdzielnosc_majatkowa else None

        # ---

        # DODANE: usuwanie powtarzajacych sie (osoba_fizyczna, rodzaj wspolnosci)
        #if osoby_fizyczne and rodzaj_wspolnosci:
        #    result = []
        #    for pair in zip(osoby_fizyczne, rodzaj_wspolnosci):
        #        if pair not in result:
        #            result.append(pair)
        #    osoby_fizyczne, rodzaj_wspolnosci = list(zip(*result))

    else:
        logger(text='[DEBUG] extract_from_ii: SCHEMAT 2: osoba_fizyczna: brak', level='DEBUG', info=info)

        osoby_fizyczne = []
        rodzaj_wspolnosci = []
        rozdzielnosc_majatkowa = None
        numer_udzialu_w_prawie = []

    # --- podmioty - inna_osoba_prawna ---

#    logger(text='[DEBUG] extract_from_ii: inna_osoba_prawna: sprawdzanie', level='DEBUG')

    podmioty_1 = find_tags_with_text(u'Inna osoba prawna', html)

    if podmioty_1:

        nazwa_podmiotu = find_with_text(u'2. Nazwa', html, col=-1)
        nazwa_podmiotu = nazwa_podmiotu[0] if nazwa_podmiotu else ''
        nazwa_podmiotu = nazwa_podmiotu.replace(u'-', u'')

        miejscowosc_podmiotu = find_with_text(u'3. Siedziba', html, col=-1)
        miejscowosc_podmiotu = miejscowosc_podmiotu[0] if miejscowosc_podmiotu else ''
        miejscowosc_podmiotu = miejscowosc_podmiotu.replace(u'-', u'')

        regon = find_with_text(u'4. REGON', html, col=-1)
        regon = regon[0] if regon else ''
        regon = regon.replace(u'-', u'')

        podmioty_1 = [{'p_nazwa_podmiotu': nazwa_podmiotu, 'p_miejscowosc_podmiotu': miejscowosc_podmiotu, 'p_regon': regon}]
        logger(text='[DEBUG] extract_from_ii: SCHEMAT 2: podmioty_1 (inna osoba_prawna): {}'.format(podmioty_1), level='DEBUG', info=info)

        # usuniecie z pustymi wszystkimi polami
        logger(text='[DEBUG] extract_from_ii: SCHEMAT 2: podmioty_1 (inna osoba_prawna): przed: {}'.format(podmioty_1), level='DEBUG', info=info)
        podmioty_1 = [item for item in podmioty_1 if any(item.values())]
        logger(text='[DEBUG] extract_from_ii: SCHEMAT 2: podmioty_1 (inna osoba_prawna): po: {}'.format(podmioty_1), level='DEBUG', info=info)
    else:
        podmioty_1 = []

    # --- podmioty - jednostka_samorzadowa ---

#    logger(text='[DEBUG] extract_from_ii: jednostka_samorzadowa: sprawdzanie', level='DEBUG')

    podmioty_2 = find_tags_with_text(u'Jednostka samorządu', html)

    if podmioty_2:
        logger(text=u'SCHEMAT 2: BRAK PRZYKLADU Z DANYMI "Jednostka samorządu"', level='ERROR', info=info)

        nazwa_podmiotu = '' #find_with_text(u'2. Nazwa', html, col=-1)
        miejscowosc_podmiotu = '' #find_with_text(u'3. Siedziba', html, col=-1)
        regon = '' #find_with_text(u'4. REGON', html, col=-1)
        podmioty_2 = [{'p_nazwa_podmiotu': nazwa_podmiotu, 'p_miejscowosc_podmiotu': miejscowosc_podmiotu, 'p_regon': regon}]

        logger(text='[DEBUG] extract_from_ii: SCHEMAT 2: podmiot_2 (jednostk samorzadowa): {}'.format(podmioty_2), level='DEBUG', info=info)

        # usuniecie z pustymi wszystkimi polami
        logger(text='[DEBUG] extract_from_ii: SCHEMAT 2: podmioty_2 (jednostk samorzadowa): przed: {}'.format(podmioty_2), level='DEBUG', info=info)
        podmioty_2 = [item for item in podmioty_2 if any(item.values())]
        logger(text='[DEBUG] extract_from_ii: SCHEMAT 2: podmioty_2 (jednostk samorzadowa): po: {}'.format(podmioty_2), level='DEBUG', info=info)

        if not any(podmioty_2):
            podmioty_2 = []
    else:
        podmioty_2 =  []

    # --- podmioty - skarb_panstwa ---

#    logger(text='[DEBUG] extract_from_ii: skarb_panstwa: sprawdzanie', level='DEBUG')

    podmioty_3 = find_tags_with_text(u'Skarb Państwa', html)

    if podmioty_3:
        logger(text=u'SCHEMAT 2: BRAK PRZYKLADU Z DANYMI "Skarb Państwa"', level='ERROR', info=info)

        nazwa_podmiotu = '' #find_with_text(u'2. Nazwa', html, col=-1)
        miejscowosc_podmiotu = '' #find_with_text(u'3. Siedziba', html, col=-1)
        regon = '' #find_with_text(u'4. REGON', html, col=-1)
        podmioty_3 = [{'p_nazwa_podmiotu': nazwa_podmiotu, 'p_miejscowosc_podmiotu': miejscowosc_podmiotu, 'p_regon': regon}]

        logger(text='[DEBUG] extract_from_ii: SCHEMAT 2: podmiot 3 (skarb panstwa): {}'.format(podmioty_3), level='DEBUG', info=info)

        # usuniecie z pustymi wszystkimi polami
        logger(text='[DEBUG] extract_from_ii: SCHEMAT 2: podmioty_3 (skarb panstwa): przed: {}'.format(podmioty_3), level='DEBUG', info=info)
        podmioty_3 = [item for item in podmioty_3 if any(item.values())]
        logger(text='[DEBUG] extract_from_ii: SCHEMAT 2: podmioty_3 (skarb panstwa): po: {}'.format(podmioty_3), level='DEBUG', info=info)

        if not any(podmioty_3):
            podmioty_3 = []
    else:
        podmioty_3 =  []

    podmioty = podmioty_1 + podmioty_2 + podmioty_3

    return osoby_fizyczne, rodzaj_wspolnosci, numer_udzialu_w_prawie, rozdzielnosc_majatkowa, podmioty

#------------------------------------------------------------------------------
# DOKUMENT IV
#------------------------------------------------------------------------------

def extract_form_iv(html, info=None):
    """
    - rodzaj hipoteki
    - numer udzialu w prawie
    - suma
    - waluta
    - osoba prawna
    """

    logger(text='[DEBUG] extract_from_iv: poczatek: HTML', level='DEBUG', info=info)

    schemat = find_tags_with_text('Rubryka 4', html)

    if schemat:
        return extract_form_iv_schemat_2(html, info)
    else:
        return extract_form_iv_schemat_1(html, info)

#------------------------------------------------------------------------------

def extract_form_iv_schemat_1(html, info=None):
    """
    - rodzaj hipoteki - lista
    - numer udzialu w prawie - lista
    - suma - lista
    - waluta - lista
    - osoba prawna - lista
    """

    # --- rodzaj hipoteki ---

    logger(text='[DEBUG] extract_from_iv: SCHEMAT 1: rodzaj_hipoteki: poczatek', level='DEBUG', info=info)

    rodzaj_hipoteki = find_tags_with_text(u'Rodzaj hipoteki', html)

    if rodzaj_hipoteki:
        rodzaj_hipoteki = [rodzaj_hipoteki] if not isinstance(rodzaj_hipoteki[0], list) else rodzaj_hipoteki
        rodzaj_hipoteki = [x[1].text for x in rodzaj_hipoteki]  # if len(x) > 1]

    #rodzaj_hipoteki = find_with_text(u'Rodzaj hipoteki', html, col=1)

    # --- suma i waluta hipoteki ---

    logger(text='[DEBUG] extract_from_iv: SCHEMAT 1: suma, waluta: poczatek', level='DEBUG', info=info)

    suma = find_tags_with_text(u'Suma', html)
    waluta = find_tags_with_text(u'Suma', html)

    if suma:
        suma = [suma] if not isinstance(suma[0], list) else suma
        waluta = [x[1].text.split(' ')[-1] for x in suma]  # if len(x) > 1]

        #suma = [x[1].text.split(' ')[0] for x in suma]  # if len(x) > 1] # problem ze spacja w '900 000,00' # test-5
        #suma = [re.match('([0-9, ]+)', x[1].get_text()).groups()[0].replace(' ','') for x in suma]  # if len(x) > 1]
        suma = [x[1].get_text().split(' (')[0].replace(' ','') for x in suma]  # if len(x) > 1]
        if args.debug:
            logger(text='[DEBUG] suma: {}'.format(suma), level='DEBUG', info=info)

    # -- udział (numer udziału w prawie) ---

    logger(text='[DEBUG] extract_from_iv: SCHEMAT 1: hipoteka_numer_udzialu_w_prawie: poczatek', level='DEBUG', info=info)

    hipoteka_numer_udzialu_w_prawie = find_tags_with_text(u'Udział (numer udziału w prawie)', html)
    if hipoteka_numer_udzialu_w_prawie:
        hipoteka_numer_udzialu_w_prawie = [hipoteka_numer_udzialu_w_prawie] if not isinstance(hipoteka_numer_udzialu_w_prawie[0], list) else hipoteka_numer_udzialu_w_prawie
        hipoteka_numer_udzialu_w_prawie = [x[2].text for x in hipoteka_numer_udzialu_w_prawie]  # if len(x) > 1]

    logger(text='[DEBUG] hipoteka_numer_udzialu_w_prawie: {}'.format(hipoteka_numer_udzialu_w_prawie), level="DEBUG", info=info)

    # --- osoba prawna ---

    logger(text='[DEBUG] extract_from_iv: SCHEMAT 1: osoba_prawna: poczatek', level='DEBUG', info=info)

    osoba_prawna = find_tags_with_text(u'Inna osoba prawna', html)

    if osoba_prawna:
        osoba_prawna = [osoba_prawna] if not isinstance(osoba_prawna[0], list) else osoba_prawna
        osoba_prawna = [x[2].text.split(',')[0] for x in osoba_prawna]  # if len(x) > 2]

        if len(osoba_prawna) != len(rodzaj_hipoteki):
            for i in range(len(osoba_prawna), len(rodzaj_hipoteki)):
                osoba_prawna.append(None)
    elif suma:
        osoba_prawna = []
        for i in range(len(suma)):
            osoba_prawna.append(None)

    return rodzaj_hipoteki, hipoteka_numer_udzialu_w_prawie, osoba_prawna, suma, waluta

#------------------------------------------------------------------------------

def extract_form_iv_schemat_2(html, info=None):
    """
    - rodzaj hipoteki
    - numer udzialu w prawie
    - suma
    - waluta
    - osoba prawna
    """

    logger(text='[DEBUG] extract_from_iv: SCHEMAT 2: poczatek: HTML', level='DEBUG', info=info)

    # --- rodzaj hipoteki ---

    rodzaj_hipoteki = find_with_text(u'Rodzaj hipoteki', html, col=-1)

    logger(text='[DEBUG] extract_from_iv: SCHEMAT 2: rodzaj_hipoteki: {}'.format(repr(rodzaj_hipoteki)), level='DEBUG', info=info)

    # --- suma hipoteki ---

    suma = find_with_text(u'Suma', html, col=-1)
    #print('suma:', suma)
    suma = suma[0::2] # co druga suma aby pominac sume slownie
    suma = [x.replace(' ', '').replace(',', '.') for x in suma] # problem ze spacja w '900 000,00' # test-5

    logger(text='[DEBUG] extract_from_iv: SCHEMAT 2: suma: {}'.format(suma), level='DEBUG', info=info)

    # --- waluta hipoteki ---

    waluta = find_with_text(u'Waluta sumy', html, col=-1)

    logger(text='[DEBUG] extract_from_iv: SCHEMAT 2: waluta: {}'.format(waluta), level='DEBUG', info=info)

    # -- udział (numer udziału w prawie) ---

    hipoteka_numer_udzialu_w_prawie = find_with_text(u'Udział', html, col=-1)

    logger(text='[DEBUG] extract_from_iv: SCHEMAT 2: hipoteka_numer_udzialu_w_prawie: {}'.format(hipoteka_numer_udzialu_w_prawie), level='DEBUG', info=info)

    # --- osoba prawna ---

    logger(text='[DEBUG] extract_from_iv: SCHEMAT 2: osoba_prawna: sprawdzanie', level='DEBUG', info=info)


    osoba_prawna = find_with_text(u'Podrubryka 4.4.4 - Inna osoba prawna', html, 'table')

    if osoba_prawna:
        nazwa_podmiotu = find_with_text(u'1. Nazwa', html, col=-1)
        #nazwa_podmiotu = nazwa_podmiotu[0] if nazwa_podmiotu else ''

        miejscowosc_podmiotu = find_with_text(u'2. Siedziba', html, col=-1)
        #miejscowosc_podmiotu = miejscowosc_podmiotu[0] if miejscowosc_podmiotu else ''

        regon = find_with_text(u'3. REGON', html, col=-1)
        #regon = regon[0] if regon else ''

        #osoba_prawna = [[nazwa_podmiotu, miejscowosc_podmiotu, regon]]
        osoba_prawna = list(zip(nazwa_podmiotu, miejscowosc_podmiotu,regon))

        logger(text='[DEBUG] extract_from_iv: osoba_prawna: {}'.format(osoba_prawna), level='DEBUG', info=info)
    else:
        osoba_prawna = [[]]

    return rodzaj_hipoteki, hipoteka_numer_udzialu_w_prawie, osoba_prawna, suma, waluta

###############################################################################
### PROGRAM GLOWNY
###############################################################################

#------------------------------------------------------------------------------
### PRZYGOTOWANIE PLIKOW DO ANALIZY ###########################################
#------------------------------------------------------------------------------

logger(text='[DEBUG] wczytywanie ID z pliku CSV: poczatek', level='DEBUG')

# loading from CSV the same IDs from previous extractions
ids_list = read_previous_ids(args.output)

logger(text='[DEBUG] wczytywanie ID z pliku CSV: koniec', level='DEBUG')

# wypisanie wczytanych ID
if args.debug:
    print('[DEBUG]: --- wczytane ID ---')
    print('\n'.join(repr(x) for x in ids_list[:10]))
    if len(ids_list) > 10:
         print('... wiecej ...')
    print('[DEBUG]: --- koniec ---')

#------------------------------------------------------------------------------

logger(text='[DEBUG] grupowanie pliki HTML ze wzgledu na ID: poczatek', level='DEBUG')

# aggregation of input files due to id
ksiegi_wieczyste_mapa = prepare_files_map(args.input)

logger(text='[DEBUG] grupowanie pliki HTML ze wzgledu na ID: koniec', level='DEBUG')

#------------------------------------------------------------------------------

logger(text='[DEBUG] usuwanie ID, ktore juz wyekstrahowano: poczatek', level='DEBUG')

# remove files that were previously extracted from further analysis
tmp = list(ksiegi_wieczyste_mapa.keys())

# ADDED: using dataframe with only `id` (ids_list) instead of dataframe with all data` results` (list (results.ids))
for i in tmp:
    if i in ids_list:
        ksiegi_wieczyste_mapa.pop(i)

logger(text='[DEBUG] usuwanie ID, ktore juz wyekstrahowano: koniec', level='DEBUG')

#------------------------------------------------------------------------------

logger(text='ilosc (nowych) ID do ekstrakcji: {}'.format(len(ksiegi_wieczyste_mapa)))

#------------------------------------------------------------------------------
### EKSTRAKCJA DANYCH #########################################################
#------------------------------------------------------------------------------

# zliczanie czasu dzialania
t_start = datetime.datetime.now()
tick = 0

#------------------------------------------------------------------------------

# lista wszystkich wierszy do zapisu dla wszystkich numerow ksiag wieczystych
all_rows = []

for k in ksiegi_wieczyste_mapa:

    logger(text='przetwarzanie {}: poczatek'.format(k), level='INFO', id=k)

    tick += 1
    if tick % 100 == 0:
        print(u'------ PROGRESS: {} z {} ...'.format(tick, len(ksiegi_wieczyste_mapa)))

    try:

        # dla kazdego ID poszukuje plikow z typem io, ii oraz iv
        io, ii, iv, io_summary, ii_summary, iv_summary = analyse_pliki_powiazane(ksiegi_wieczyste_mapa[k]['pliki_powiazane'])

        #logger(text='io: {}'.format(io), level='DEBUG')
        #logger(text='ii: {}'.format(ii), level='DEBUG')
        #logger(text='iv: {}'.format(iv), level='DEBUG')

        is_error = False

        # string z nazwami powiazanych plikow
        tmp_nazwy_plikow = ", ".join(list(set([x['nazwa_pliku'].keys()[0] for x in io + ii + iv])))

        numery_ksiag = []
        typy_nieruchomosci = []

        if (len(ii) > 0) and (len(iv) > 0):

            #is_error = False

            #----------------------------------------------------------------------------------------------------------
            # string z nazwami powiazanych plikow
            #tmp_nazwy_plikow = ", ".join(list(set([x['nazwa_pliku'].keys()[0] for x in io + ii + iv])))

            #----------------------------------------------------------------------------------------------------------

            # lista numerow ksiag z powiazanych plikow
            # numery_ksiag = list(set(io_summary['numery_ksiag'] + ii_summary['numery_ksiag'] + iv_summary['numery_ksiag']))
            # DODANE: poprzednia wersja nie trzymala kolejnosci - najwazniejszy jest numer z io_summary['numery_ksiag']
            numery_ksiag = []
            for data in [io_summary['numery_ksiag'] + ii_summary['numery_ksiag'] + iv_summary['numery_ksiag']]:
                for numer in data:
                    if numer not in numery_ksiag:
                        numery_ksiag.append(numer)
            #print(numery_ksiag)

            # za duzo numerow ksiag
            if len(numery_ksiag) > 1:
                logger(k, tmp_nazwy_plikow, '', u'pliki zawieraja niespojne numery ksiag ({})'.format(','.join(numery_ksiag)), 'ERROR')
                # warunkowe dodanie do pliku wynikowego
                if not args.allow:
                    numery_ksiag = ['']
                    is_error = True

            # za malo numerow ksiag
            elif len(numery_ksiag) == 0:
                logger(k, tmp_nazwy_plikow, '', u'brak numerow ksiag'.format(','.join(numery_ksiag)), 'ERROR')
                if not args.allow:
                    numery_ksiag = ['']
                    is_error = True

            else:
                # sprawdzenie czy numer ksiegi pasuje do nazwy plikow
                if k != '-'.join(re.sub('^-', '', re.sub('[-]+', '-', re.sub('[^a-zA-Z0-9]', '-', numery_ksiag[0].replace('NR ', '')))).split('-')[:3]).upper():
                    logger(k, tmp_nazwy_plikow, '', u'ID pliku niespojne z numerem ksiegi ({})'.format(numery_ksiag[0]), 'WARNING')

            #----------------------------------------------------------------------------------------------------------

            typy_nieruchomosci = list(set(io_summary['typy_nieruchomosci'] + ii_summary['typy_nieruchomosci'] + iv_summary['typy_nieruchomosci']))

            if len(typy_nieruchomosci) > 1:
                logger(k, tmp_nazwy_plikow, numery_ksiag[0], u'pliki zawieraja niespojne typy nieruchomosci ({})'.format(','.join(typy_nieruchomosci)), 'ERROR')
                #is_error = True

            elif len(typy_nieruchomosci) == 0:
                logger(k, tmp_nazwy_plikow, numery_ksiag[0], u'brak typu nieruchomosci')
                if not args.allow:
                    is_error = True

            else:
                # if re.findall(re.compile('|'.join(typy_nieruchomosci_out)), typy_nieruchomosci[0]):
                #     is_error = True
                #     logger(k, '', numery_ksiag[0], u'typ nieruchomosci out ({})'.format(','.join(typy_nieruchomosci)))
                if re.findall(re.compile('|'.join(typy_nieruchomosci_in)), typy_nieruchomosci[0]) is None:
                    logger(k, tmp_nazwy_plikow, numery_ksiag[0], u'niezdefiniowany typ nieruchomosci ({})'.format(','.join(typy_nieruchomosci)))
                    if not args.allow:
                        is_error = True

        elif (len(ii) == 0) and (len(iv) == 0):
            if not args.allow:
                is_error = True
            logger(k, '', '', u'brak plikow II i IV', "ERROR")

        elif len(ii) == 0:
            if not args.allow:
                is_error = True
            logger(k, '', '', u'brak pliku II', "ERROR")

        elif len(iv) == 0:
            if not args.allow:
                is_error = True
            logger(k, '', '', u'brak pliku IV', "ERROR")

        else:
            if not args.allow:
                is_error = True
            logger(k, '', '', u'dla danego ID zaden z plikow nie spelnia kryterium wlaczenia', "ERROR")

        #----------------------------------------------------------------------

        if is_error:
            logger(k, '', '', u'pomijanie ksiegi z powodu bledu', "ERROR")
        elif len(numery_ksiag) == 0:
            logger(k, '', '', u'pomijanie ksiegi z powodu braku numeru ksiegi', "ERROR")
        else:
            # wlasciwa ekstrakcja danych

            if len(io) == 0:
                logger(k, tmp_nazwy_plikow, numery_ksiag[0], u'brak pliku I-O', 'WARNING')
            elif len(io) > 1:
                logger(k, tmp_nazwy_plikow, numery_ksiag[0], u'dzial I-O znajduje sie w wiecej niz jednym pliku - wybieram pierwszy z brzegu')
                io = [io[0]]

            if len(ii) > 1:
                logger(k, tmp_nazwy_plikow, numery_ksiag[0], u'dzial II znajduje sie w wiecej niz jednym pliku - wybieram pierwszy z brzegu')
                ii = [ii[0]]

            if len(iv) > 1:
                logger(k, tmp_nazwy_plikow, numery_ksiag[0], u'dzial IV znajduje sie w wiecej niz jednym pliku - wybieram pierwszy z brzegu')
                iv = [iv[0]]

            if len(io) > 0:
                miejscowosc, ulica, numer_budynku, \
                numer_lokalu, przeznaczenie_lokalu, \
                liczba_pokoi, kondygnacja, pole_powierzchni_uzytkowej = extract_from_io(io[0]['html'], io[0])
            else:
                miejscowosc, ulica, numer_budynku, \
                numer_lokalu, przeznaczenie_lokalu, \
                liczba_pokoi, kondygnacja, pole_powierzchni_uzytkowej = None, None, None, None, None, None, None, None

            logger(text='[DEBUG] IO: [miejscowosc, ulica, numer_budynku, numer_lokalu, przeznaczenie_lokalu, liczba_pokoi, kondygnacja, pole_powierzchni_uzytkowej]', level='DEBUG', id=k)
            logger(text='[DEBUG] IO: ilosc: {}'.format([len(miejscowosc or []), len(ulica or []), len(numer_budynku or []), len(numer_lokalu or []), len(przeznaczenie_lokalu or []), len(liczba_pokoi or []), len(kondygnacja or []), len(pole_powierzchni_uzytkowej or [])]), level='DEBUG', id=k)
            logger(text='[DEBUG] IO: {}'.format([miejscowosc, ulica, numer_budynku, numer_lokalu, przeznaczenie_lokalu, liczba_pokoi, kondygnacja, pole_powierzchni_uzytkowej]), level='DEBUG', id=k)

            if len(ii) > 0:
                osoby_fizyczne, rodzaj_wspolnosci, numer_udzialu_w_prawie, rozdzielnosc_majatkowa, podmioty = extract_from_ii(ii[0]['html'], ii[0])
            else:
                osoby_fizyczne, rodzaj_wspolnosci, numer_udzialu_w_prawie, rozdzielnosc_majatkowa, podmioty = [], None, None, None, []

            logger(text='[DEBUG] II: [osoby_fizyczne, rozdzielnosc_majatkowa, podmioty]', level='DEBUG', id=k)
            logger(text='[DEBUG] II: ilosc: {}'.format([len(osoby_fizyczne), len(rozdzielnosc_majatkowa or []), len(podmioty)]), level='DEBUG', id=k)
            logger(text='[DEBUG] II: {}'.format([osoby_fizyczne, rozdzielnosc_majatkowa, podmioty]), level='DEBUG', id=k)

            if len(iv) > 0:
                rodzaj_hipoteki, hipoteka_numer_udzialu_w_prawie, osoba_prawna, suma, waluta = extract_form_iv(iv[0]['html'], iv[0])
            else:
                rodzaj_hipoteki, hipoteka_numer_udzialu_w_prawie, osoba_prawna, suma, waluta = None, None, None, None, None


            if hipoteka_numer_udzialu_w_prawie is None and rodzaj_hipoteki:
                if numer_udzialu_w_prawie is None:
                    logger(text="TODO: if numer_udzialu_w_prawie is None:", level='DEBUG', id=k)
                else:
                    #powielenie hipotek aby do kazdej osoby przypisac wszystkie hipoteki
                    osoby = list(set(numer_udzialu_w_prawie))
                    result = []
                    for x in osoby:
                        result += [x] * len(rodzaj_hipoteki)
                    hipoteka_numer_udzialu_w_prawie = result

                    if rodzaj_hipoteki:
                        rodzaj_hipoteki = rodzaj_hipoteki * len(osoby)
                    if osoba_prawna:
                        osoba_prawna = osoba_prawna * len(osoby)
                    if suma:
                        suma = suma * len(osoby)
                    if waluta:
                        waluta = waluta * len(osoby)

            logger(text='[DEBUG] IV: [rodzaj_hipoteki, hipoteka_numer_udzialu_w_prawie, osoba_prawna, suma, waluta]', level='DEBUG', id=k)
            logger(text='[DEBUG] IV: ilosc: {}'.format([len(rodzaj_hipoteki or []), len(hipoteka_numer_udzialu_w_prawie or []), len(osoba_prawna or []), len(suma or []), len(waluta or [])]), level='DEBUG', id=k)
            logger(text='[DEBUG] IV: {}'.format([rodzaj_hipoteki, hipoteka_numer_udzialu_w_prawie, osoba_prawna, suma, waluta]), level='DEBUG', id=k)

            ###
            ### ZAPIS
            ###

            record = {
                'id': k,
                'plik_io': io[0]['nazwa_pliku'].keys()[0] if io else None,
                'plik_ii': ii[0]['nazwa_pliku'].keys()[0] if ii else None,
                'plik_iv': iv[0]['nazwa_pliku'].keys()[0] if iv else None,
                'numer_ksiegi_wieczystej': numery_ksiag[0],

                'miejscowosc': miejscowosc,
                'ulica': ulica,
                'numer_budynku': numer_budynku,
                'numer_lokalu': numer_lokalu,
                'przeznaczenie_lokalu': przeznaczenie_lokalu,
                'liczba_pokoi': liczba_pokoi,
                'kondygnacja': kondygnacja,
                'pole_powierzchni_uzytkowej': pole_powierzchni_uzytkowej,

                # puste miejsca jako wartosci domyslne
                'p_nazwa_podmiotu': '',
                'p_miejscowosc_podmiotu': '',
                'p_regon': '',

                'os_imie1': '',
                'os_imie2': '',
                'os_nazwisko': '',
                'os_ojciec': '',
                'os_matka': '',
                'os_pesel': '',
                'os_data_urodzenia': '',
                'os_rodzaj_wspolnosci': '',
                'rozdzielnosc_majatkowa': '',
                'os_numer_udzialu_w_prawie': '',

                'h_rodzaj': '',
                'h_suma': '',
                'h_waluta': '',
                'h_osoba_prawna': '',
                'wojewodztwo': '',
            }

            kod = record['id'].upper().split('-')[0]
            if kod in wojewodztwa:
                record['wojewodztwo'] = wojewodztwa[kod][1] 
                
            # DODANE: dla kazdego podmiotu bedzie osobny wiersz
            #all_records = [record]

            all_records = []

            if podmioty:
                for x in podmioty:
                    temp_record = copy.deepcopy(record)
                    temp_record['p_nazwa_podmiotu'] = x['p_nazwa_podmiotu']
                    temp_record['p_miejscowosc_podmiotu'] = x['p_miejscowosc_podmiotu']
                    temp_record['p_regon'] = x['p_regon']
                    all_records.append(temp_record)

            # DODANE: dla kazdej osoby fizycznej bedzie osobny wiersz
            if osoby_fizyczne:
                for item in osoby_fizyczne:

                    #logger(text='osoba: {}'.format(repr(item)), level='DEBUG')
                    temp_record = copy.deepcopy(record)
                    temp_record['os_imie1']    = item['os_imie1']
                    temp_record['os_imie2']    = item['os_imie2']
                    temp_record['os_nazwisko'] = item['os_nazwisko']
                    temp_record['os_ojciec']   = item['os_ojciec']
                    temp_record['os_matka']    = item['os_matka']
                    temp_record['os_pesel']    = item['os_pesel']
                    temp_record['os_data_urodzenia'] = item['os_data_urodzenia']
                    temp_record['os_rodzaj_wspolnosci'] = item['os_rodzaj_wspolnosci']
                    temp_record['os_numer_udzialu_w_prawie'] = item['os_numer_udzialu_w_prawie']
                    temp_record['rozdzielnosc_majatkowa'] = rozdzielnosc_majatkowa
                    all_records.append(temp_record)

            if rodzaj_hipoteki:
                all_temp_records = []
                for record in all_records:
                     bez_hipoteki = True
                     for x, y, z, w, q in zip(rodzaj_hipoteki, suma, waluta, osoba_prawna, hipoteka_numer_udzialu_w_prawie):

                        #logger(text='[DEBUG] osoba: {} | {} | {} | {} | {}'.format(record['os_numer_udzialu_w_prawie'], repr(q), record['os_numer_udzialu_w_prawie'] == q, repr(w),  repr(record['os_nazwisko'])), level='DEBUG')

                        if record['os_numer_udzialu_w_prawie'] == q:
                            bez_hipoteki = False
                            temp_record = copy.deepcopy(record)
                            temp_record['h_numer_udzialu_w_prawie'] = q
                            temp_record['h_rodzaj'] = x
                            temp_record['h_suma']   = y
                            temp_record['h_waluta'] = z
                            if isinstance(w, (list, tuple)):
                                temp_record['h_osoba_prawna'] = w[0]
                            else:
                                temp_record['h_osoba_prawna'] = w
                            all_temp_records.append(temp_record)
                     if bez_hipoteki:
                        all_temp_records.append(record)

                all_records = all_temp_records

            # dodanie pustego rekordu
            if len(all_records) == 0 and args.allow:
                all_records = [record]

            all_rows += all_records

            logger(text='ilosc dodanych wierszy: {}'.format(len(all_records)), id=k)

            if len(osoby_fizyczne) == 0 and len(podmioty) == 0 and len(ii) > 0:
                logger(k, ii[0]['nazwa_pliku'].keys()[0], numery_ksiag[0], u'nie odnaleziono osoby fizycznej ani podmiotu')

    except Exception as e:
        logger(k, '', '', 'nieobsluzony blad - {}'.format(e), 'ERROR')
        # DODANE: wypisywanie calego komunikaty o bledzie
        if args.debug:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            traceback.print_exception(exc_type, exc_value, exc_traceback, limit=2, file=sys.stdout)

    logger(text='przetwarzanie {}: koniec'.format(k), level='INFO', id=k)

#------------------------------------------------------------------------------

# zliczanie czasu dzialania + podsumowanie
t_stop = datetime.datetime.now()
t_diff = t_stop - t_start
logger(text='Czas wykonania {} - wydajnosc {} minut na 100 000 ID ksiag'.format(t_diff, round( ((t_diff.total_seconds()/(len(ksiegi_wieczyste_mapa) + 1))*100000/60), 1)))

#------------------------------------------------------------------------------
### ZAPIS #####################################################################
#------------------------------------------------------------------------------

logger(text='[DEBUG] save', level='DEBUG')

all_rows = pd.DataFrame.from_dict(data=all_rows, orient='columns')

if args.debug:
    print(all_rows.head(10))

if len(all_rows) > 0:

    if os.path.exists(args.output):
        # appending without headers (header = None) in column order (columns=...)
        all_rows.to_csv(args.output, index=False, sep=';', mode='a', encoding='utf-8', columns=COLUMNS, header=None)
    else:
        # creating a new header file in column order (columns=...)
        all_rows.to_csv(args.output, index=False, sep=';', mode='a', encoding='utf-8', columns=COLUMNS)

