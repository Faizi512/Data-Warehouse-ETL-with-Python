import mysql.connector
import re

con = mysql.connector.connect(host="localhost", user="Moazzam", passwd="12345", db="Data")
cursor = con.cursor()

All_Authors = []
ArticlePK = 0


#  -------------------- getting Tables Data Start --------------------------
def getAllAuthors():
    # it returns all in form of dictionaries
    # cursor = con.cursor(buffered=True, dictionary=True)
    # it returns all in form of tuples
    cursor = con.cursor(buffered=True, dictionary=False)
    cursor.execute("select id,AuthorName from author")
    con.commit()
    return cursor.fetchall()


def getAllAuthorKeyword():
    cursor = con.cursor(buffered=True, dictionary=False)
    cursor.execute("select id,AuthorKeyword from AuthorKeyword")
    con.commit()
    return cursor.fetchall()


def getAllJournalKeyword():
    cursor = con.cursor(buffered=True, dictionary=False)
    cursor.execute("select id,JournalKeyword from JournalKeyword")
    con.commit()
    return cursor.fetchall()


def getAllJournal():
    cursor = con.cursor(buffered=True, dictionary=False)
    cursor.execute("select id,JournalName from Journal")
    con.commit()
    return cursor.fetchall()


def getAllWc():
    cursor = con.cursor(buffered=True, dictionary=False)
    cursor.execute("select id,WC_Name from WebOfScience")
    con.commit()
    return cursor.fetchall()


def getAllAffiliations():
    cursor = con.cursor(buffered=True, dictionary=False)
    cursor.execute("select id,Full_Affiliation from Affiliation")
    con.commit()
    return cursor.fetchall()


def getAllResearchAreas():
    cursor = con.cursor(buffered=True, dictionary=False)
    cursor.execute("select id,Area from ResearchArea")
    con.commit()
    return cursor.fetchall()

def splitAff(affiliation):
    ValList = []
    for f in affiliation:
        # print(f[1])
        vals = f[1].rsplit(',',3)
        if len(vals) == 4:
            ValList.append((vals[0].strip(), vals[1].strip(), vals[2].strip(), vals[3].strip(), f[0]))
        elif len(vals) == 3:
            ValList.append(('', vals[0].strip(), vals[1].strip(), vals[2].strip(), f[0]))
        elif len(vals) == 2:
            ValList.append(('', '', vals[0].strip(), vals[1].strip(), f[0]))
        elif len(vals) == 1:
            ValList.append(('', '', '', vals.strip(), f[0]))

        # print(ValList)
    return ValList

#  -------------------- getting Tables Data End --------------------------

#  -------------------- getting Matching PKs of items Start--------------------------
def getAuthId(name, authors):
    # a = authors[:5]
    # print(f"{name.lower().capitalize()},{a[0]}")
    for i in range(len(authors)):
        if name.lower().capitalize() in authors[i][1]:
            return authors[i][0]


def getAuthKeyId(name, athorkeys):
    for i in range(len(athorkeys)):
        if name in athorkeys[i][1]:
            return athorkeys[i][0]


def getJrlKeyId(name, journalkeys):
    for i in range(len(journalkeys)):
        if name in journalkeys[i][1]:
            return journalkeys[i][0]


def getJrlId(name, journals):
    for i in range(len(journals)):
        if name in journals[i][1]:
            return journals[i][0]


def getWcId(name, Wc):
    for i in range(len(Wc)):
        if name in Wc[i][1]:
            return Wc[i][0]


def getAffId(name, Aff):
    # print(name)
    for i in range(len(Aff)):
        if name.strip() in Aff[i][1].strip():
            # print(f"=> {Aff[i][1].strip()} , {Aff[i][0]}")
            return Aff[i][0]


def getResearchId(name, Area):
    for i in range(len(Area)):
        if name in Area[i][1]:
            return Area[i][0]


#  -------------------- getting Matching PKs of items End--------------------------

#  -------------------- All Tables Mappings Started --------------------------

def Map_Authors(records_list, Authors_list):
    mappings = set()
    print("Authors Mapping Started...")
    #  it gives access to change value of global variable in local function
    global ArticlePK

    for record in records_list:
        ArticlePK += 1
        if '' in record['AF']:
            record['AF'].pop()
        for af in record['AF']:

            if ',' in af:
                temp = af.split(',')
                af = temp[1] + ' ' + temp[0]

            mappings.add((ArticlePK, getAuthId(af.lower().strip().capitalize(), Authors_list)))

    cursor.executemany("insert into article_author(ArticleID,AuthorID) values (%s,%s)", mappings)
    con.commit()
    mappings.clear()
    print("Authors Mapping Completed...")


def Map_AuthorKeywords(records_list, AuthorKeys_list):
    mappings = set()
    print("AuthorKeywords Mapping Started...")
    #  it gives access to change value of global variable in local function
    global ArticlePK

    for record in records_list:
        ArticlePK += 1
        for de in record['DE']:
            mappings.add((ArticlePK, getAuthKeyId(de.strip().lower(), AuthorKeys_list)))

    cursor.executemany("insert into article_authorkeyword(ArticleID,AuthorKeywordID) values (%s,%s)", mappings)
    con.commit()
    mappings.clear()
    print("AuthorKeywords Mapping Completed...")


def Map_JournalKeywords(records_list, JournalKeys_list):
    mappings = set()
    print("JournalKeys Mapping Started...")
    #  it gives access to change value of global variable in local function
    global ArticlePK

    for record in records_list:
        ArticlePK += 1
        for id_ in record['ID']:
            mappings.add((ArticlePK, getJrlId(id_.strip().capitalize(), JournalKeys_list)))

    cursor.executemany("insert into article_journalkeyword(ArticleID,JournalKeywordID) values (%s,%s)", mappings)
    con.commit()
    mappings.clear()
    print("JournalKeys Mapping Completed...")


def Map_Journals(records_list, Journals_list):
    mappings = set()
    print("Journals Mapping Started...")
    #  it gives access to change value of global variable in local function
    global ArticlePK

    for record in records_list:
        ArticlePK += 1
        for so in record['SO']:
            mappings.add((ArticlePK, getJrlId(so.strip().capitalize(), Journals_list)))

    cursor.executemany("insert into article_journal(ArticleID,JournalID) values (%s,%s)", mappings)
    con.commit()
    mappings.clear()
    print("Journals Mapping Completed...")


def Map_WebOfScience(records_list, Wc_list):
    mappings = set()
    print("WebOfScience Mapping Started...")
    #  it gives access to change value of global variable in local function
    global ArticlePK

    for record in records_list:
        ArticlePK += 1
        for wc in record['WC']:
            mappings.add((ArticlePK, getWcId(wc.strip().capitalize(), Wc_list)))

    cursor.executemany("insert into article_WebOfScience(ArticleID,WcID) values (%s,%s)", mappings)
    con.commit()
    mappings.clear()
    print("WebOfScience Mapping Completed...")


def Map_ResearchArea(records_list, Research_list):
    mappings = set()
    print("ResearchArea Mapping Started...")
    #  it gives access to change value of global variable in local function
    global ArticlePK

    for record in records_list:
        ArticlePK += 1
        for sc in record['SC']:
            mappings.add((ArticlePK, getResearchId(sc.strip().capitalize(), Research_list)))

    cursor.executemany("insert into article_research(ArticleID,ResearchID) values (%s,%s)", mappings)
    con.commit()
    mappings.clear()
    print("ResearchArea Mapping Completed...")


def Map_Article_Author_Affiliation(records_list, Author_list, Affiliation_list):
    mappings = set()
    print("Article_Author_Affiliation Mapping Started...")
    #  it gives access to change value of global variable in local function
    global ArticlePK

    for record in records_list:
        ArticlePK += 1
        if '' in record['C1']:
            record['C1'].pop()
        for c1 in record['C1']:
            res = c1_splitting(c1)
            authList = res[0]

            for index in range(len(authList)):
                if ',' in authList[index]:
                    temp = authList[index].split(',')
                    authList[index] = temp[1].strip() + ' ' + temp[0].strip()

            # print(f"{authList}")
            affId = getAffId(res[1].strip(), Affiliation_list)

            for i in authList:
                # (ArticlePK, getAuthId(i.strip(), Author_list), affId)
                # print(f"==> {(ArticlePK, getAuthId(i.strip(), Author_list), affId)}")
                mappings.add((ArticlePK, getAuthId(i.strip(), Author_list), affId))

    cursor.executemany("insert into article_author_affiliation(ArticleID,AuthorID,AffiliationID) values (%s,%s,%s)",
                       mappings)
    con.commit()
    mappings.clear()
    print("Article_Author_Affiliation Mapping Completed...")


#  -------------------- All Tables Mappings Ended --------------------------


def c1_splitting(c1):
    # print(f"origional Aff : {c1}")

    if ']' not in c1:
        return ['Missing'], c1
    else:
        c1 = c1.replace('[', '')
        temp = c1.split(']')
        authors = re.split(';', temp[0])

        # print(f"""
        # temp[0] : {temp[0]}
        # temp[1] : {temp[1]}
        # authors : {authors}
        # """)

        return authors, temp[1].strip()

def updateAffiliation(aff_list):

    print("Affiliation Updation Started...")



    cursor.executemany("""
    UPDATE Affiliation SET Institute_Name=%s, Department_Name=%s, City=%s, Country=%s
    WHERE id=%s
    """, (aff_list))
    con.commit()

    print("Affiliation Updation Completed...")