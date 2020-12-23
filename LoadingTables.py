# "AF","TI", "SO" ,  "LA" , "DT" "DE"   "ID"   "AB"   "NR"   "TC"   "U1"  "U2"  "PY"   "BP"  "EP"  "WC"  "SC"  "C1"
import re
from distutils.command.config import config

#  pip install pymysql (install)

import mysql.connector

con = mysql.connector.connect(host="localhost", user="Moazzam", passwd="12345", db="Data")
cursor = con.cursor()

articles_ = []
authors_ = []
authors_key_ = []
journals_key_ = []
journals_ = []
WC_ = []
SC_ = []
C1_ = []

author_duplicate = set()
authorkey_duplicate = set()
journalkey_duplicate = set()
journal_duplicate = set()
WC_duplicate = set()
SC_duplicate = set()
C1_duplicate = set()


def extract_attr(f, pos, currentLine, match):
    temp = ''
    if currentLine[:2] == match:
        temp = currentLine[3:]
        currentLine = f.readline()
        while currentLine[:2] == '  ':
            temp += currentLine[3:]
            currentLine = f.readline()

    return pos, temp


def get_numbers_from_string(line):
    num = re.findall('\\d+', line)
    if len(num) != 0:
        return num[0]
    else:
        return '0'


def closeConnections(records):
    if cursor and con:
        cursor.close()
        con.close()


def load_articles(r):
    for i in r:
        articles_.append((i['TI'].strip(), i['AB'].strip(), get_numbers_from_string(i['U1']), i['DT'],
                          get_numbers_from_string(i['BP']), get_numbers_from_string(i['EP'])
                          , i['FileName'], i['LA'], get_numbers_from_string(i['NR']), get_numbers_from_string(i['PY']),
                          get_numbers_from_string(i['U2']), get_numbers_from_string(i['TC'])
                          ))
    cursor.executemany("""
                    insert into article(Title,Abstract,Days180Downloads,DocType,StartPage,EndPage,FileName,Language,
                    NoOfRef,PublishYear,Since2013Downloads,TimesCited)
                    values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)      
                    """, articles_)
    con.commit()
    articles_.clear()

    print(f"==> Articles loaded & saving...")


def load_authors(r):
    print(f"==> Authors loaded & saving...")
    for i in r:
        if '' in i['AF']:
            i['AF'].pop()
        for author in i['AF']:

            if ',' in author:
                temp = author.split(',')
                author = temp[1] + ' ' + temp[0]
            author = author.lower().strip().capitalize()
            if author not in author_duplicate:
                authors_.append((author, i['FileName']))
                author_duplicate.add(author)

    cursor.executemany("insert into author(AuthorName,FileName) values (%s,%s)", authors_)
    con.commit()
    authors_.clear()
    print(f"==> Authors loaded & saving completed...")


def load_authorkeywords(r):
    print(f"==> Author Keywords loaded & saving...")
    for i in r:
        for authorkey in i['DE']:
            auth_key = authorkey.strip()
            if auth_key.lower() not in authorkey_duplicate:
                authors_key_.append((auth_key.capitalize(), i['FileName']))
                authorkey_duplicate.add(auth_key.lower())
    cursor.executemany("insert into AuthorKeyword(AuthorKeyword,FileName) values (%s,%s)", authors_key_)
    con.commit()
    authors_key_.clear()
    print(f"==> Author Keywords loaded & saving Completed...")


def load_journalkeywords(r):
    print(f"==> Journal Keywords loaded & saving...")
    for i in r:
        for journalkey in i['ID']:
            jrl_key = journalkey.strip()
            if jrl_key.lower() not in journalkey_duplicate:
                journals_key_.append((jrl_key.capitalize(), i['FileName']))
                journalkey_duplicate.add(jrl_key.lower())

    cursor.executemany("insert into JournalKeyword(JournalKeyword,FileName) values (%s,%s)", journals_key_)
    con.commit()
    journals_key_.clear()
    print(f"==> Journal Keywords loaded & saving Completed...")


def load_journal(r):
    print(f"==> journal loaded & saving...")
    for i in r:
        for journal in i['SO']:
            jrl = journal.strip()
            if jrl.lower() not in journal_duplicate:
                journals_.append((jrl.capitalize(), i['FileName']))
                journal_duplicate.add(jrl.lower())

    cursor.executemany("insert into Journal(JournalName,FileName) values (%s,%s)", journals_)
    con.commit()
    journals_.clear()
    print(f"==> journal loaded & saving Completed...")


def load_woc(r):
    print(f"==> Wc loaded & saving...")
    for i in r:
        for Wc in i['WC']:
            wc = Wc.strip()
            if wc.lower() not in WC_duplicate:
                WC_.append((wc.capitalize(), i['FileName']))
                WC_duplicate.add(wc.lower())

    cursor.executemany("insert into WebOfScience(WC_Name,FileName) values (%s,%s)", WC_)
    con.commit()
    WC_.clear()
    print(f"==> Wc loaded & saving Completed...")


def load_research(r):
    print(f"==> Sc loaded & saving...")
    for i in r:
        for Sc in i['SC']:
            sc = Sc.strip()
            if sc.lower() not in SC_duplicate:
                SC_.append((sc.capitalize(), i['FileName']))
                SC_duplicate.add(sc.lower())

    cursor.executemany("insert into ResearchArea(Area,FileName) values (%s,%s)", SC_)
    con.commit()
    SC_.clear()
    print(f"==> Sc loaded & saving Completed...")


def load_affiliation(r):
    print(f"==> Affiliation loaded & saving...")
    for i in r:
        i['C1'] = extracting_affilation_String(i)
        for C1 in i['C1']:
            c1 = C1.strip()
            if c1.lower() not in C1_duplicate:
                C1_.append((c1, "Missing", "Missing", "Missing", "Missing", i['FileName']))
                C1_duplicate.add(c1.lower())

    cursor.executemany(
        "insert into Affiliation(Full_Affiliation,Institute_Name,Department_Name,City,Country,FileName) values (%s,%s,%s,%s,%s,%s)",
        C1_)
    con.commit()
    C1_.clear()
    print(f"==> Affiliation loaded & saving Completed...")


def extracting_affilation_String(records):
    for i in range(len(records['C1'])):
        if ']' in records['C1'][i]:
            records['C1'][i] = records['C1'][i].split(']')[1].strip()

    for j in records['C1']:
        if '' in records['C1']:
            records['C1'].pop()
    return records['C1']
