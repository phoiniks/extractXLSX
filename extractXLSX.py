#!/usr/bin/python3

from sys import stdout, argv
from os import remove
import xlrd
import sqlite3
from os.path import splitext
from time import localtime, strftime, time
import logging
from functools import wraps, partial

logFile = ".".join([splitext(argv[0])[0], "log"])
logFile = logFile.upper()

format = "%(name)s %(levelname)s um %(asctime)s: %(message)s, function %(funcName)s"

timeFormat = "%H:%M:%S %d.%m.%Y"

logging.basicConfig(filename=logFile, filemode="w", level=logging.DEBUG, format=format, datefmt=timeFormat)

files = " ".join([argv[0], argv[1]])

log = logging.getLogger(files)

log.info("BEGINN")

t_start = time()

xl = xlrd.open_workbook(argv[1])

sheet = xl.sheet_by_index(1)

log.setLevel(logging.INFO)

con = sqlite3.connect("customer.db")
cur = con.cursor()

# sql = "DROP TABLE customer"
# cur.execute(sql)

sql = "CREATE TABLE IF NOT EXISTS customer (id INTEGER PRIMARY KEY, wkn TEXT, provision REAL, gueltig_bis TEXT, lokalzeit DATE DEFAULT (DATETIME('now', 'localtime')))"
cur.execute(sql)

sql = "INSERT INTO customer (wkn, provision, gueltig_bis) VALUES(?, ?, ?)"

print("\n%d Reihen gefunden. Extrahiere Werte. Wenn dass Programm ordnungsgemäß durchläuft,\nfindet es eine Reihe mehr (Kopfzeile der Tabelle) als Werte im Excel-Arbeitsblatt vorhanden sind!" %sheet.nrows)


def timethis(func):
    '''
    Dieser Dekorator gibt die Ausführungszeit aus.
    '''
    @wraps(func)
    def wrapper(*args, **kwargs):
        start = time()
        result = func(*args, **kwargs)
        end = time()
        print(func.__name__, end - start, end='\n\n\n')
        log.info("%s: %f" %(func.__name__, end - start))
        sql = "CREATE TABLE IF NOT EXISTS timethis(id INTEGER PRIMARY KEY, funktion TEXT, zeit REAL, lokalzeit DATE DEFAULT (DATETIME('now', 'localtime')))"
        cur.execute(sql)
        sql = "INSERT INTO timethis (funktion, zeit) VALUES(?, ?)"
        cur.execute(sql, (func.__name__, end - start))
        con.commit()

        return result

    return wrapper


@timethis
def extract(sheet):
    count = 1
    for idx in range(1, sheet.nrows, 1):
        values = sheet.row_values(idx)
        wkn = values[2]
        provision = values[19] * 100
        gueltig_bis = strftime("%d.%m.%Y", localtime((values[20]-25569)*86400)) # berechnet Lokalzeit aus Microsoft-Zeit
        if count == 1:
            stdout.write(" Erster Eintrag: %6d|%6s|%4s|%10s \n" %(count, wkn, provision, gueltig_bis))
        else:
            stdout.write("Letzter Eintrag: %6d|%6s|%4s|%10s \r" %(count, wkn, provision, gueltig_bis))
        log.info("%d|%s|%s|%s" %(count, wkn, provision, gueltig_bis))
        count += 1
        yield (values[2], values[19]*100, strftime("%d.%m.%Y", localtime((values[20]-25569)*86400))) # -"-


print("\n")

log.info("PROGRAMMSTART -- NUR, WENN DIESE AUSGABE AUF 'ENDE' ENDET, IST DAS PROGRAMM AUCH DURCHGELAUFEN!")
cur.executemany(sql, extract(sheet))
cur.execute("SELECT COUNT(id), * FROM customer GROUP BY provision")

print("\n\n")

liste = cur.fetchall()
vals = [[str(e) for e in list(el)] for el in liste]

keys = ["Anzahl in Gruppe: ", "Beispiel: ", "WKN: ", "Provision: ", "Gültig bis: ", "Lokalzeit: "]

for v in vals:
    values = []

    for el in v:
        values.append(el)

    output = []
    output = map(list, zip(keys, values))
    output = "   ".join(["".join(list(item)) for item in output])

    print(output, "\n")

cur.execute("select avg(provision) from customer");
average = cur.fetchone()

print("\nDurchschnittliche Provision: %.2f Prozent\n" %average)

con.commit()

t_end = time()

dauer = t_end - t_start

print("Gesamtbearbeitungsdauer (Dekompression, Extraktion, Speichern in SQLite3): %.2f Sekunden\n" %dauer)

log.info("Gesamtbearbeitungsdauer (Dekompression, Extraktion, Speichern in SQLite3): %.2f Sekunden\n" %dauer)

log.info("ENDE")
