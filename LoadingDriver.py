from AttributeExtraction import get_abs_path, get_all_txt_files, read_file, record_extract_from_text_file, get_data
from TablesMapping import Map_Authors, getAllAuthors
from LoadingTables import load_authors, load_articles, load_authorkeywords, load_journal, load_journalkeywords, \
    load_research, load_woc, load_affiliation
import cmath
AllPaths = []

# get path of all the files in AllPaths
for i in get_abs_path():
    # print(i)
    for j in get_all_txt_files(i):
        AllPaths.append(j)
        # print(j)

# for p in AllPaths:
#     print(p)
# Path = "C:\\Users\\Muazzam\\PycharmProjects\\DataWareHouse\\Web of Science_new\\2008\\1-500.txt";
sum = 0
articleIDCounter = 0
num = []
for i in AllPaths:
    d = get_data(i)
    sum += len(d)

    # INSERTING DATA IN BASE TABLES

    load_articles(d)
    load_authors(d)
    load_authorkeywords(d)
    load_journalkeywords(d)
    load_journal(d)
    load_woc(d)
    load_research(d)
    load_affiliation(d)

    print("-------")
    print(f"#{sum} records Extracted!")
    print("-------")

# print("Saving ...")

print("Completed")

print(max(num))
