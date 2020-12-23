from AttributeExtraction import get_abs_path, get_all_txt_files, get_data
from TablesMapping import Map_Authors, getAllAuthors, getAllJournal, Map_Journals, Map_ResearchArea, \
    getAllResearchAreas, getAllAffiliations, Map_Article_Author_Affiliation, splitAff, updateAffiliation, \
    Map_WebOfScience, getAllWc, Map_AuthorKeywords, Map_JournalKeywords, getAllAuthorKeyword, getAllJournalKeyword

AllPaths = []

# get path of all the files in AllPaths
for i in get_abs_path():
    # print(i)
    for j in get_all_txt_files(i):
        AllPaths.append(j)
        # print(j)

ListofLoadedAuthors = getAllAuthors()
ListOfAuthorKeys = getAllAuthorKeyword()
ListOfJournalKeys = getAllJournalKeyword()
ListofLoadedJournals = getAllJournal()
ListofAllWC = getAllWc()
ListofLoadedresearchAreas = getAllResearchAreas()
ListofLoadedAffiliations = getAllAffiliations()

sum = 0
for i in AllPaths:

    d = get_data(i)
    sum += len(d)

    # # CREATING MAPPINGS (WEAK ENTITIES)

    Map_Authors(d, ListofLoadedAuthors)
    Map_AuthorKeywords(d, ListOfAuthorKeys)
    Map_JournalKeywords(d, ListOfJournalKeys)
    Map_Journals(d, ListofLoadedJournals)
    Map_WebOfScience(d, ListofAllWC)
    Map_ResearchArea(d, ListofLoadedresearchAreas)
    Map_ResearchArea(d, ListofLoadedresearchAreas)
    Map_Article_Author_Affiliation(d, ListofLoadedAuthors, ListofLoadedAffiliations)



    print("-------")
    print(f"#{sum} records Extracted!")
    print("-------")

# print("Saving ...")

updateAffiliation(splitAff(ListofLoadedAffiliations))

print("Completed")