# -*- coding: utf-8 -*-

import os
import re
import datetime
import pandas as pd
from bs4 import BeautifulSoup
from io import open

### ZMIENNE DO ZMIANY PRZEZ UZYTKOWNIKA

sciezka_do_katalogu_z_plikami = '/Users/macbook/Documents/konwersja/ksiegi'
sciezka_do_projektu = '/Users/macbook/Documents/konwersja'

### ZMIENNE GLOBALNE

sciezka_do_pliku_wyjsciowego = sciezka_do_projektu + '/output/ksiegi_wieczyste.csv'
sciezka_do_katalogu_z_logami = sciezka_do_projektu + '/output/logi'

if not os.path.exists('/'.join(sciezka_do_pliku_wyjsciowego.split('/')[:-1])):
    os.makedirs('/'.join(sciezka_do_pliku_wyjsciowego.split('/')[:-1]))

if not os.path.exists(sciezka_do_katalogu_z_logami):
    os.makedirs(sciezka_do_katalogu_z_logami)

typy_nieruchomosci_in = [u'STANOWIĄCY ODRĘBNĄ NIERUCHOMOŚĆ', u'PRAWO DO LOKALU',
                         u'NIERUCHOMOŚĆ GRUNTOWA', u'GRUNT ODDANY W UŻYTKOWANIE WIECZYSTE']
# typy_nieruchomosci_out = [u'NIERUCHOMOŚĆ GRUNTOWA', u'GRUNT ODDANY W UŻYTKOWANIE WIECZYSTE']

logs = []

### FUNKCJE POMOCNICZE


def logger(id, fn, numer, text, type='INFO'):
    print(u'{} : {} : {} : {} : {}'.format(type, id, fn, numer, text))
    return [{'type': type, 'id': id, 'fn': fn, 'numer': numer, 'text': text}]


def read_previous_results(path):
    log = []

    if os.path.isfile(path):
        results = pd.read_csv(path, low_memory=False, encoding='utf-8', sep=';')
    else:
        log += logger('', '', '', 'nie odnaleziono pliku z wynikami poprzednich ekstrakcji - zostanie utworzony nowy plik')
        results = pd.DataFrame({'id': [],
                                'plik_io': [],
                                'plik_ii': [],
                                'plik_iv': [],
                                'numer_ksiegi_wieczystej': [],
                                'miejscowosc': [],
                                'ulica': [],
                                'numer_budynku': [],
                                'numer_lokalu': [],
                                'przeznaczenie_lokalu': [],
                                'liczba_pokoi': [],
                                'kondygnacja': [],
                                'pole_powierzchni_uzytkowej': [],
                                'p1_nazwa_podmiotu': [],
                                'p1_miejscowosc_podmiotu': [],
                                'p1_regon': [],
                                'os1_imie1': [],
                                'os1_imie2': [],
                                'os1_nazwisko': [],
                                'os1_ojciec': [],
                                'os1_matka': [],
                                'os1_pesel': [],
                                'os1_data_urodzenia': [],
                                'os1_rodzaj_wspolnosci': [],
                                'os2_imie1': [],
                                'os2_imie2': [],
                                'os2_nazwisko': [],
                                'os2_ojciec': [],
                                'os2_matka': [],
                                'os2_pesel': [],
                                'os2_data_urodzenia': [],
                                'os2_rodzaj_wspolnosci': [],
                                'os3_imie1': [],
                                'os3_imie2': [],
                                'os3_nazwisko': [],
                                'os3_ojciec': [],
                                'os3_matka': [],
                                'os3_pesel': [],
                                'os3_data_urodzenia': [],
                                'os3_rodzaj_wspolnosci': [],
                                'rozdzielnosc_majatkowa': [],
                                'h1_rodzaj': [],
                                'h1_suma': [],
                                'h1_waluta': [],
                                'h1_osoba_prywatna': [],
                                'h2_rodzaj': [],
                                'h2_suma': [],
                                'h2_waluta': [],
                                'h2_osoba_prywatna': [],
                                'h3_rodzaj': [],
                                'h3_suma': [],
                                'h3_waluta': [],
                                'h3_osoba_prywatna': [],
                                'h4_rodzaj': [],
                                'h4_suma': [],
                                'h4_waluta': [],
                                'h4_osoba_prywatna': [],
                                'h5_rodzaj': [],
                                'h5_suma': [],
                                'h5_waluta': [],
                                'h5_osoba_prywatna': [],
                                'h6_rodzaj': [],
                                'h6_suma': [],
                                'h6_waluta': [],
                                'h6_osoba_prywatna': [],
                                'h7_rodzaj': [],
                                'h7_suma': [],
                                'h7_waluta': [],
                                'h7_osoba_prywatna': [],
                                'h8_rodzaj': [],
                                'h8_suma': [],
                                'h8_waluta': [],
                                'h8_osoba_prywatna': [],
                                'h9_rodzaj': [],
                                'h9_suma': [],
                                'h9_waluta': [],
                                'h9_osoba_prywatna': [],
                                'h10_rodzaj': [],
                                'h10_suma': [],
                                'h10_waluta': [],
                                'h10_osoba_prywatna': []},
                               columns=['id', 'plik_io', 'plik_ii', 'plik_iv', 'numer_ksiegi_wieczystej', 'miejscowosc',
                                        'ulica', 'numer_budynku', 'numer_lokalu', 'przeznaczenie_lokalu',
                                        'liczba_pokoi', 'kondygnacja',
                                        'pole_powierzchni_uzytkowej', 'p1_nazwa_podmiotu', 'p1_miejscowosc_podmiotu',
                                        'p1_regon', 'os1_imie1', 'os1_imie2', 'os1_nazwisko',
                                        'os1_ojciec', 'os1_matka', 'os1_pesel', 'os1_data_urodzenia',
                                        'os1_rodzaj_wspolnosci', 'os2_imie1', 'os2_imie2', 'os2_nazwisko', 'os2_ojciec',
                                        'os2_matka', 'os2_pesel', 'os2_data_urodzenia', 'os2_rodzaj_wspolnosci',
                                        'os3_imie1', 'os3_imie2', 'os3_nazwisko', 'os3_ojciec', 'os3_matka',
                                        'os3_pesel', 'os3_data_urodzenia', 'os3_rodzaj_wspolnosci',
                                        'rozdzielnosc_majatkowa', 'h1_rodzaj', 'h1_suma', 'h1_waluta',
                                        'h1_osoba_prawna', 'h2_rodzaj', 'h2_suma', 'h2_waluta', 'h2_osoba_prawna',
                                        'h3_rodzaj', 'h3_suma', 'h3_waluta', 'h3_osoba_prawna', 'h4_rodzaj',
                                        'h4_suma', 'h4_waluta', 'h4_osoba_prawna', 'h5_rodzaj', 'h5_suma',
                                        'h5_waluta', 'h5_osoba_prawna', 'h6_rodzaj', 'h6_suma', 'h6_waluta',
                                        'h6_osoba_prywatna', 'h7_rodzaj', 'h7_suma', 'h7_waluta', 'h7_osoba_prawna',
                                        'h8_rodzaj', 'h8_suma', 'h8_waluta', 'h8_osoba_prawna', 'h9_rodzaj',
                                        'h9_suma', 'h9_waluta', 'h9_osoba_prawna', 'h10_rodzaj', 'h10_suma',
                                        'h10_waluta', 'h10_osoba_prawna'])
    return results, log


def prepare_files_map(path):

    log = []

    # uwzgledniam zarowno htm jak i html - uwzgledniam podfoldery
    lista_plikow = [{name: root} for root, dirs, files in os.walk(path) for name in files if name.endswith((".html", ".htm"))]

    # generowanie id w oparciu o nazwe pliku
    # wszystkie znaki nie alfanumeryczne sa zamieniane na pauze (-)
    # wielokrotne pauzy zamieniam na pojedyncza oraz usuwam NR jako przedrostek
    # zakladam format id [A-Z0-9]-[0-9]-[0-9]
    slownik_nazw_plikow = {}
    for x in lista_plikow:
        try:
            id = '-'.join(re.sub('^-', '', re.sub('[-]+', '-', re.sub('[^a-zA-Z0-9]', '-', x.keys()[0].replace('NR ', '')))).split('-')[:3]).upper()
            slownik_nazw_plikow.update({x.keys()[0]: {'id': id, 'root': x.values()[0]}})
        except:
            log += logger('', x.keys()[0], '', u'nie mozna wczytac pliku ({})'.format(x.values()[0]), 'ERROR')

    lista_id_plikow = list(set([x['id'] for x in slownik_nazw_plikow.values()]))

    # kazdemu identyfikatorowi przypisuje informacje o powiazanych z nim plikach
    ksiegi_wieczyste_mapa = {}
    for i in lista_id_plikow:
        ksiegi_wieczyste_mapa.update({i: {'pliki_powiazane': [{k: v['root']} for k, v in slownik_nazw_plikow.items() if v['id'] == i]}})

    return ksiegi_wieczyste_mapa, log


def analyse_pliki_powiazane(pliki_powiazane):

    html_io = []
    html_ii = []
    html_iv = []
    log = []

    for t in pliki_powiazane:
        with open(t.values()[0] + '/' + t.keys()[0], 'r', encoding='utf-8') as f:
            html = BeautifulSoup(f.read(), 'html.parser')

        try:
            typ_pliku = html.select('.csTTytul')
            typ_pliku = typ_pliku[0].text if typ_pliku else None
        except:
            log += logger('', t.keys()[0], '', u'nie mozna odczytac typu pliku z pliku', 'ERROR')
            typ_pliku = None

        if typ_pliku not in [u'DZIAŁ I-O - OZNACZENIE NIERUCHOMOŚCI',
                             u'DZIAŁ II - WŁASNOŚĆ',
                             u'DZIAŁ IV - HIPOTEKA']:
            log += logger('', t.keys()[0], '', u'plik zawiera informacje o innym dziale niz zdefiniowane - {}'.format(typ_pliku), 'WARNING')
            typ_pliku = None

        try:
            numer_ksiegi_wieczystej = html.h2
            numer_ksiegi_wieczystej = numer_ksiegi_wieczystej.b.text if numer_ksiegi_wieczystej else None
            if numer_ksiegi_wieczystej:
                numer_ksiegi_wieczystej = re.sub('[/]+', '/', re.sub('[^a-zA-Z0-9]', '/',
                                                                     numer_ksiegi_wieczystej.replace('NR ', '')))
        except:
            log += logger('', t.keys()[0], '', u'nie mozna odczytac numeru ksiegi wieczystej z pliku', 'ERROR')
            numer_ksiegi_wieczystej = None

        try:
            typ_nieruchomosci = html.h3
            typ_nieruchomosci = typ_nieruchomosci.text if typ_nieruchomosci else None
        except:
            log += logger('', t.keys()[0], '', u'nie mozna odczytac typu nieruchomosci z pliku', 'ERROR')
            typ_nieruchomosci = None

        if typ_pliku and numer_ksiegi_wieczystej and typ_nieruchomosci:
            if typ_pliku == u'DZIAŁ I-O - OZNACZENIE NIERUCHOMOŚCI':
                html_io += [{'html': html, 'numer_ksiegi_wieczystej': numer_ksiegi_wieczystej,
                             'typ_nieruchomosci': typ_nieruchomosci, 'nazwa_pliku': t}]
            elif typ_pliku == u'DZIAŁ II - WŁASNOŚĆ':
                html_ii = [{'html': html, 'numer_ksiegi_wieczystej': numer_ksiegi_wieczystej,
                            'typ_nieruchomosci': typ_nieruchomosci, 'nazwa_pliku': t}]
            elif typ_pliku == u'DZIAŁ IV - HIPOTEKA':
                html_iv = [{'html': html, 'numer_ksiegi_wieczystej': numer_ksiegi_wieczystej,
                            'typ_nieruchomosci': typ_nieruchomosci, 'nazwa_pliku': t}]

    html_io_summary = {'numery_ksiag': [x['numer_ksiegi_wieczystej'] for x in html_io if x['numer_ksiegi_wieczystej']],
                       'typy_nieruchomosci': [x['typ_nieruchomosci'] for x in html_io if x['typ_nieruchomosci']]}
    html_ii_summary = {'numery_ksiag': [x['numer_ksiegi_wieczystej'] for x in html_ii if x['numer_ksiegi_wieczystej']],
                       'typy_nieruchomosci': [x['typ_nieruchomosci'] for x in html_ii if x['typ_nieruchomosci']]}
    html_iv_summary = {'numery_ksiag': [x['numer_ksiegi_wieczystej'] for x in html_iv if x['numer_ksiegi_wieczystej']],
                       'typy_nieruchomosci': [x['typ_nieruchomosci'] for x in html_iv if x['typ_nieruchomosci']]}

    return html_io, html_ii, html_iv, html_io_summary, html_ii_summary, html_iv_summary, log


def find_tags_with_text(text, html):
    try:
        el = [x.find_all('td') for x in html.find_all(lambda tag: tag.name == 'tr' and text in tag.text)]
        el = el[0] if len(el) == 1 else None if len(el) == 0 else el
    except:
        el = None
    return el


def extract_from_io(html):

    czy_lokal = find_tags_with_text(u'Lokal', html)

    if czy_lokal:

        miejscowosc = find_tags_with_text(u'Położenie', html)
        if miejscowosc:
            miejscowosc = miejscowosc[3].text.split(', ') if len(miejscowosc) > 3 else [None]
            miejscowosc = miejscowosc[0] if len(miejscowosc) <= 3 else miejscowosc[3]

        ulica = find_tags_with_text(u'Ulica', html)
        ulica = ulica[3].text if ulica else None

        numer_budynku = find_tags_with_text(u'Numer budynku', html)
        numer_budynku = numer_budynku[4].text if numer_budynku else None

        numer_lokalu = find_tags_with_text(u'Numer lokalu', html)
        numer_lokalu = numer_lokalu[5].text if numer_lokalu else None

        przeznaczenie_lokalu = find_tags_with_text(u'Przeznaczenie lokalu', html)
        przeznaczenie_lokalu = przeznaczenie_lokalu[1].text if przeznaczenie_lokalu else None

        liczba_pokoi = find_tags_with_text(u'Opis lokalu', html)
        if liczba_pokoi:
            liczba_pokoi = liczba_pokoi[1].text.replace('\r\n', '').replace('\n', ' ').split(', ')
            liczba_pokoi = [x for x in liczba_pokoi if x.find(u'POKÓJ') > -1]
            liczba_pokoi = sum([int(x.split(' - ')[-1]) if x.find(' - ') > -1 else 1 for x in
                                liczba_pokoi])  # jak nie ma znaku - to - licze jako 1
        else:
            liczba_pokoi = None

        kondygnacja = find_tags_with_text(u'Kondygnacja', html)
        kondygnacja = kondygnacja[1].text if kondygnacja else None

        pole_powierzchni_uzytkowej = find_tags_with_text(u'Pole powierzchni', html)
        pole_powierzchni_uzytkowej = pole_powierzchni_uzytkowej[1].text if pole_powierzchni_uzytkowej else None

    else:
        miejscowosc, ulica, numer_budynku, \
        numer_lokalu, przeznaczenie_lokalu, liczba_pokoi, \
        kondygnacja, pole_powierzchni_uzytkowej = None, None, None, None, None, None, None, None,

    return miejscowosc, ulica, numer_budynku, numer_lokalu, przeznaczenie_lokalu, liczba_pokoi, kondygnacja, pole_powierzchni_uzytkowej


def extract_osoba_fizyczna(osoba_fizyczna):
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
        else:
            ojciec = None
            matka = None
        pesel = [x for x in osoba_fizyczna if re.findall('[0-9]{11}', x)]
        pesel = pesel[0] if pesel else None
        data_urodzenia = (
            pesel[4:6] + '-' + pesel[2:4] + '-19' + pesel[:2] if int(pesel[2:4]) < 20 else pesel[4:6] + '-' + str(
                int(pesel[2:4]) - 20) + '-20' + pesel[:2]) if pesel else None
    else:
        imie1 = None
        imie2 = None
        nazwisko = None
        ojciec = None
        matka = None
        pesel = None
        data_urodzenia = None

    return imie1, imie2, nazwisko, ojciec, matka, pesel, data_urodzenia


def extract_podmiot(podmiot):
    if podmiot:
        nazwa_podmiotu = podmiot[0]
        miejscowosc_podmiotu = podmiot[1] if len(podmiot) > 1 else None
        regon = podmiot[2] if len(podmiot) > 2 else None
    else:
        nazwa_podmiotu = None
        miejscowosc_podmiotu = None
        regon = None

    return nazwa_podmiotu, miejscowosc_podmiotu, regon


def extract_from_ii(html):
    osoby_fizyczne = find_tags_with_text(u'Osoba fizyczna', html)
    if osoby_fizyczne:
        if not isinstance(osoby_fizyczne[0], list):
            osoby_fizyczne = [osoby_fizyczne]
        osoby_fizyczne = [extract_osoba_fizyczna(x[1].text.split(', ')) for x in osoby_fizyczne]

        rodzaj_wspolnosci = find_tags_with_text(u'Lista wskazań udziałów w prawie', html)
        if not isinstance(rodzaj_wspolnosci[0], list):
            rodzaj_wspolnosci = [rodzaj_wspolnosci]
        rodzaj_wspolnosci = [x[4].text.replace('---', '-') if len(x) > 4 else None for x in rodzaj_wspolnosci]

        rozdzielnosc_majatkowa = find_tags_with_text(u'ROZDZIELNOŚĆ MAJĄTKOWA', html)
        rozdzielnosc_majatkowa = 'TAK' if rozdzielnosc_majatkowa else None
    else:
        osoby_fizyczne = []
        rodzaj_wspolnosci = []
        rozdzielnosc_majatkowa = None

    podmioty_1 = find_tags_with_text(u'Inna osoba prawna', html)
    if podmioty_1:
        if not isinstance(podmioty_1[0], list):
            podmioty_1 = [podmioty_1]
        podmioty_1 = [extract_podmiot(x[1].text.split(', ')) for x in podmioty_1]
    else:
        podmioty_1 = []

    podmioty_2 = find_tags_with_text(u'Jednostka samorządu', html)
    if podmioty_2:
        if not isinstance(podmioty_2[0], list):
            podmioty_2 = [podmioty_2]
        podmioty_2 = [extract_podmiot(x[1].text.split(', ')) for x in podmioty_2]
    else:
        podmioty_2 = []
        
    podmioty_3 = find_tags_with_text(u'Skarb Państwa', html)
    if podmioty_3:
        if not isinstance(podmioty_3[0], list):
            podmioty_3 = [podmioty_3]
        podmioty_3 = [extract_podmiot(x[1].text.split(', ')) for x in podmioty_3]
    else:
        podmioty_3 = []

    podmioty = podmioty_1 + podmioty_2 + podmioty_3

    return osoby_fizyczne, rodzaj_wspolnosci, rozdzielnosc_majatkowa, podmioty


def extract_form_iv(html):
    rodzaj_hipoteki = find_tags_with_text(u'Rodzaj hipoteki', html)
    if rodzaj_hipoteki:
        rodzaj_hipoteki = [rodzaj_hipoteki] if not isinstance(rodzaj_hipoteki[0], list) else rodzaj_hipoteki
        rodzaj_hipoteki = [x[1].text for x in rodzaj_hipoteki]  # if len(x) > 1]
    suma = find_tags_with_text(u'Suma', html)
    waluta = find_tags_with_text(u'Suma', html)

    if suma:
        suma = [suma] if not isinstance(suma[0], list) else suma
        waluta = [x[1].text.split(' ')[-1] for x in suma]  # if len(x) > 1]
        suma = [x[1].text.split(' ')[0] for x in suma]  # if len(x) > 1]

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

    return rodzaj_hipoteki, osoba_prawna, suma, waluta

##################
### PROGRAM GLOWNY
##################

### PRZYGOTOWANIE PLIKOW DO ANALIZY

# wczytywanie wynikow poprzednich ekstrakcji
results, log = read_previous_results(sciezka_do_pliku_wyjsciowego)
# logs += log

# agregowanie plikow wejsciowych ze wzgledu na id
ksiegi_wieczyste_mapa, log = prepare_files_map(sciezka_do_katalogu_z_plikami)
logs += log

# usuwam z dalszej analizy id plikow, ktore zostaly wczesniej wyekstrahowane
tmp = list(ksiegi_wieczyste_mapa.keys())
for i in tmp:
    if i in list(results.id):
        ksiegi_wieczyste_mapa.pop(i)
l = logger('', '', '', 'do ekstrakcji wlaczono {} id'.format(len(ksiegi_wieczyste_mapa)))

### EKSTRAKCJA DANYCH

# zliczanie czasu dzialania
t_start = datetime.datetime.now()
tick = 0

tmp_results = []
for k in ksiegi_wieczyste_mapa:

    tick += 1
    if tick % 100 == 0:
        print(u'------ PROGRESS: {} z {} ...'.format(tick, len(ksiegi_wieczyste_mapa)))

    try:

        # dla kazdego id poszukuje plikow z typem io, ii oraz iv
        io, ii, iv, io_summary, ii_summary, iv_summary, log = analyse_pliki_powiazane(ksiegi_wieczyste_mapa[k]['pliki_powiazane'])
        logs += log

        if (len(ii) > 0) and (len(iv) > 0):
            is_error = False
            tmp_nazwy_plikow = ", ".join(list(set([x['nazwa_pliku'].keys()[0] for x in io + ii + iv])))

            numery_ksiag = list(set(io_summary['numery_ksiag'] + ii_summary['numery_ksiag'] + iv_summary['numery_ksiag']))
            if len(numery_ksiag) > 1:
                numery_ksiag = ['']
                is_error = True
                logs += logger(k, tmp_nazwy_plikow, '', u'pliki zawieraja niespojne numery ksiag ({})'.format(','.join(numery_ksiag)), 'ERROR')
            elif len(numery_ksiag) == 0:
                numery_ksiag = ['']
                is_error = True
            else:
                if k != '-'.join(re.sub('^-', '', re.sub('[-]+', '-', re.sub('[^a-zA-Z0-9]', '-', numery_ksiag[0].replace('NR ', '')))).split('-')[:3]).upper():
                    logs += logger(k, tmp_nazwy_plikow, '', u'id pliku niespojne z numerem ksiegi ({})'.format(numery_ksiag[0]), 'WARNING')

            typy_nieruchomosci = list(set(io_summary['typy_nieruchomosci'] + ii_summary['typy_nieruchomosci'] + iv_summary['typy_nieruchomosci']))
            if len(typy_nieruchomosci) > 1:
                is_error = True
                logs += logger(k, tmp_nazwy_plikow, numery_ksiag[0], u'pliki zawieraja niespojne typy nieruchomosci ({})'.format(','.join(typy_nieruchomosci)), 'ERROR')
            elif len(typy_nieruchomosci) == 0:
                is_error = True
                logs += logger(k, tmp_nazwy_plikow, numery_ksiag[0], u'brak typu nieruchomosci')
            else:
                # if re.findall(re.compile('|'.join(typy_nieruchomosci_out)), typy_nieruchomosci[0]):
                #     is_error = True
                #     logs += logger(k, '', numery_ksiag[0], u'typ nieruchomosci out ({})'.format(','.join(typy_nieruchomosci)))
                if re.findall(re.compile('|'.join(typy_nieruchomosci_in)), typy_nieruchomosci[0]) is None:
                    is_error = True
                    logs += logger(k, tmp_nazwy_plikow, numery_ksiag[0], u'niezdefiniowany typ nieruchomosci ({})'.format(','.join(typy_nieruchomosci)))
        elif (len(ii) == 0) and (len(iv) == 0):
            is_error = True
            logs += logger(k, '', '', u'brak plików II i IV', "ERROR")
        elif len(ii) == 0:
            is_error = True
            logs += logger(k, '', '', u'brak pliku II', "ERROR")
        elif len(iv) == 0:
            is_error = True
            logs += logger(k, '', '', u'brak pliku IV', "ERROR")
        else:
            is_error = True
            logs += logger(k, '', '', u'dla danego ID zaden z plikow nie spelnia kryterium wlaczenia', "ERROR")

        # wlasciwa ekstrakcja danych
        if not is_error:

            if len(io) == 0:
                logs += logger(k, tmp_nazwy_plikow, numery_ksiag[0], u'brak pliku I-O', 'WARNING')
            elif len(io) > 1:
                logs += logger(k, tmp_nazwy_plikow, numery_ksiag[0], u'dzial I-O znajduje sie w wiecej niz jednym pliku - wybieram pierwszy z brzegu')
                io = [io[0]]

            if len(ii) > 1:
                logs += logger(k, tmp_nazwy_plikow, numery_ksiag[0], u'dzial II znajduje sie w wiecej niz jednym pliku - wybieram pierwszy z brzegu')
                ii = [ii[0]]

            if len(iv) > 1:
                logs += logger(k, tmp_nazwy_plikow, numery_ksiag[0], u'dzial IV znajduje sie w wiecej niz jednym pliku - wybieram pierwszy z brzegu')
                iv = [iv[0]]

            if len(io) > 0:
                miejscowosc, ulica, numer_budynku, \
                numer_lokalu, przeznaczenie_lokalu, \
                liczba_pokoi, kondygnacja, pole_powierzchni_uzytkowej = extract_from_io(io[0]['html'])
            else:
                miejscowosc, ulica, numer_budynku, \
                numer_lokalu, przeznaczenie_lokalu, \
                liczba_pokoi, kondygnacja, pole_powierzchni_uzytkowej = None, None, None, None, None, None, None, None

            if len(ii) > 0:
                osoby_fizyczne, rodzaj_wspolnosci, rozdzielnosc_majatkowa, podmioty = extract_from_ii(ii[0]['html'])
            else:
                osoby_fizyczne, rodzaj_wspolnosci, rozdzielnosc_majatkowa, podmioty = [], None, None, []

            if len(iv) > 0:
                rodzaj_hipoteki, osoba_prawna, suma, waluta = extract_form_iv(iv[0]['html'])
            else:
                rodzaj_hipoteki, osoba_prawna, suma, waluta = None, None, None, None

            ###
            ### ZAPIS
            ###

            rekord = {'id': k,
                      'plik_io': io[0]['nazwa_pliku'].keys()[0] if io else None,
                      'plik_ii': ii[0]['nazwa_pliku'].keys()[0] if ii else None,
                      'plik_iv': iv[0]['nazwa_pliku'].keys()[0] if iv else None,
                      'numer_ksiegi_wieczystej': numery_ksiag[0]}

            rekord.update({'miejscowosc': miejscowosc,
                           'ulica': ulica,
                           'numer_budynku': numer_budynku,
                           'numer_lokalu': numer_lokalu,
                           'przeznaczenie_lokalu': przeznaczenie_lokalu,
                           'liczba_pokoi': liczba_pokoi,
                           'kondygnacja': kondygnacja,
                           'pole_powierzchni_uzytkowej': pole_powierzchni_uzytkowej})

            if osoby_fizyczne:
                i = 0
                for x, y in zip(osoby_fizyczne, rodzaj_wspolnosci):
                    i += 1
                    rekord.update({'os%s_imie1' % i: x[0],
                                   'os%s_imie2' % i: x[1],
                                   'os%s_nazwisko' % i: x[2],
                                   'os%s_ojciec' % i: x[3],
                                   'os%s_matka' % i: x[4],
                                   'os%s_pesel' % i: x[5],
                                   'os%s_data_urodzenia' % i: x[6],
                                   'os%s_rodzaj_wspolnosci' % i: y})
                rekord.update({'rozdzielnosc_majatkowa': rozdzielnosc_majatkowa})
            if podmioty:
                i = 0
                for x in podmioty:
                    i += 1
                    rekord.update({'p%s_nazwa_podmiotu' % i: x[0],
                                   'p%s_miejscowosc_podmiotu' % i: x[1],
                                   'p%s_regon' % i: x[2]})
            if len(osoby_fizyczne) == 0 and len(podmioty) == 0 and len(ii) > 0:
                logs += logger(k, ii[0]['nazwa_pliku'].keys()[0], numery_ksiag[0], u'nie odnaleziono osoby fizycznej ani podmiotu')

            if rodzaj_hipoteki:
                i = 0
                for x, y, z, w in zip(rodzaj_hipoteki, suma, waluta, osoba_prawna):
                    i += 1
                    rekord.update({'h%s_rodzaj' % i: x,
                                   'h%s_suma' % i: y,
                                   'h%s_waluta' % i: z,
                                   'h%s_osoba_prawna' % i: w})

            tmp_results += [rekord]
    except Exception as e:
        logs += logger(k, '', '', 'nieobsluzony blad - {}'.format(e), 'ERROR')

# zliczanie czasu dzialania + podsumowanie
t_stop = datetime.datetime.now()
print('Czas wykonania {} - wydajnosc {} minut na 100 000 id ksiag'.format((t_stop - t_start), round(float((t_stop - t_start).seconds)/(len(ksiegi_wieczyste_mapa) + 1)*100000/60), 1))

### ZAPIS

tmp_results = pd.DataFrame.from_dict(data=tmp_results, orient='columns')
if len(tmp_results) > 0:
    results = pd.concat([results, tmp_results], sort=False)
    results.to_csv(sciezka_do_pliku_wyjsciowego, index=False, sep=';', encoding='utf-8')

logs = pd.DataFrame.from_dict(data=logs, orient='columns')
if len(logs) > 0:
    logs = logs[['type', 'id', 'fn', 'numer', 'text']]
    logs.to_csv(sciezka_do_katalogu_z_logami + '/logi_' + datetime.datetime.now().strftime('%Y%m%d%H%M%S') + '.csv', index=False, sep=';', encoding='utf-8')