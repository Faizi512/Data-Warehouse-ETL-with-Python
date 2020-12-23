from mysql import connector

db_name = ''

con = connector.connect(host="localhost", user="Moazzam", passwd="12345")
cursor = con.cursor()


def getconnection(db):
    return connector.connect(host="localhost", user="Moazzam", passwd="12345", database=db)


def create_db(dbName):
    db_name = dbName
    cursor.execute(f"create database {dbName}")


def create_author_tbl():
    cursor.execute(f"use {db_name}")
    cursor.execute("""
    create table Author
    (
    id int primary key AUTO_INCREMENT,
    AuthorName text,
    FileName varchar(255)
    )
    """)


def create_article_tbl():
    cursor.execute(f"use {db_name}")
    cursor.execute("""
        create table Article
        (
        id int primary key AUTO_INCREMENT,
        Title text not null,
        Abstract text,
        Days180Downloads int,
        DocType varchar(255),
        StartPage int,
        EndPage int,
        FileName varchar(255),
        Language varchar(255),
        NoOfRef int,
        PublishYear int,
        Since2013Downloads int,
        TimesCited int
        )
        """)


def create_authorkeyword_tbl():
    cursor.execute(f"use {db_name}")
    cursor.execute("""
    create table AuthorKeyword
    (
    id int primary key AUTO_INCREMENT,
    AuthorKeyword varchar(255),
    FileName varchar(255)
    )
    """)


def create_journalkeyword_tbl():
    cursor.execute(f"use {db_name}")
    cursor.execute("""
    create table JournalKeyword
    (
    id int primary key AUTO_INCREMENT,
    JournalKeyword varchar(255),
    FileName varchar(255)
    )
    """)


def create_journal_tbl():
    cursor.execute(f"use {db_name}")
    cursor.execute("""
    create table Journal
    (
    id int primary key AUTO_INCREMENT,
    JournalName varchar(1000),
    FileName varchar(255)
    )
    """)


def create_WebOfScience_tbl():
    cursor.execute(f"use {db_name}")
    cursor.execute("""
    create table WebOfScience
    (
    id int primary key AUTO_INCREMENT,
    WC_Name varchar(255),
    FileName varchar(255)
    )
    """)


def create_Affiliation_tbl():
    cursor.execute(f"use {db_name}")
    cursor.execute("""
    create table Affiliation
    (
    id int primary key AUTO_INCREMENT,
    Full_Affiliation text,
    Institute_Name varchar(255),
    Department_Name varchar(255),
    City varchar(255),
    Country varchar(255),
    FileName varchar(255)
    )
    """)


def create_research_tbl():
    cursor.execute(f"use {db_name}")
    cursor.execute("""
    create table ResearchArea
    (
    id int primary key AUTO_INCREMENT,
    Area varchar(255),
    FileName varchar(255)
    )
    """)


# cursor.execute("drop table Article")

def article_author():
    cursor.execute(f"use {db_name}")
    cursor.execute("""
    create table article_author
    (
    id int primary key AUTO_INCREMENT,
    ArticleID int,
    AuthorID int
    )
    """)


def article_journal():
    cursor.execute(f"use {db_name}")
    cursor.execute("""
    create table article_journal
    (
    id int primary key AUTO_INCREMENT,
    ArticleID int,
    JournalID int
    )
    """)


def article_webofscience():
    cursor.execute(f"use {db_name}")
    cursor.execute("""
    create table article_WebOfScience
    (
    id int primary key AUTO_INCREMENT,
    ArticleID int,
    WcID int
    )
    """)


def article_authorkeyword():
    cursor.execute(f"use {db_name}")
    cursor.execute("""
    create table article_authorkeyword
    (
    id int primary key AUTO_INCREMENT,
    ArticleID int,
    AuthorKeywordID int
    )
    """)


def article_journalkeyword():
    cursor.execute(f"use {db_name}")
    cursor.execute("""
    create table article_journalkeyword
    (
    id int primary key AUTO_INCREMENT,
    ArticleID int,
    JournalKeywordID int
    )
    """)


def article_author_affiliation():
    cursor.execute(f"use {db_name}")
    cursor.execute("""
    create table article_author_affiliation
    (
    id int primary key AUTO_INCREMENT,
    ArticleID int,
    AuthorID int,
    AffiliationID int
    )
    """)

def author_affiliation():
    cursor.execute(f"use {db_name}")
    cursor.execute("""
    create table author_affiliation
    (
    id int primary key AUTO_INCREMENT,
    AuthorID int,
    AffiliationID int
    )
    """)


def article_research():
    cursor.execute(f"use {db_name}")
    cursor.execute("""
        create table article_research
        (
        id int primary key AUTO_INCREMENT,
        ArticleID int,
        ResearchID int
        )
        """)


cursor.execute("drop database Data")
create_db("Data")
db_name = "Data"

create_author_tbl()
create_article_tbl()
create_authorkeyword_tbl()
create_journalkeyword_tbl()
create_journal_tbl()
create_WebOfScience_tbl()
create_Affiliation_tbl()
create_research_tbl()

author_affiliation()
article_author()
article_journal()
article_webofscience()
article_authorkeyword()
article_journalkeyword()
article_author_affiliation()
article_research()
