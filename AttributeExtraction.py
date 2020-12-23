import os
import glob

# path = os.getcwd();

# gets the path of working directory from Tools import extract_attr
import re

from LoadingTables import extract_attr


def get_abs_path():
    path = os.path.dirname(os.path.abspath(__file__)) + '\Web of Science_new/'
    pathList = []
    for i in range(2008, 2020):
        pathList.append(path + str(i))
    return pathList


# get all the only .txt files from within a folder
def get_all_txt_files(path):
    files = [f for f in glob.glob(path + "**/*.txt", recursive=True)]
    return files


# Reads a .txt File
def read_file(path):
    try:
        with open(path, encoding="utf8") as file:
            Data = file.read()
    except FileNotFoundError:
        Data = None
    # SplitData = Data.split("\n")
    return Data


# Extract All records from with in a .txt file
def record_extract_from_text_file(r):
    temp = ''
    records = []
    for i in r:
        if i[:2] == "ER":
            records.append(temp)
            temp = ''
        else:
            temp += "\n" + i

    return records


def get_data(path):
    ExtractedAttrDicList = []

    af = '';
    ti = '';
    so = '';
    la = '';
    dt = '';
    de = '';
    id_ = '';
    ab = '';
    nr = '';
    tc = '';
    u1 = '';
    u2 = '';
    py = '';
    bp = '';
    ep = '';
    wc = '';
    sc = '';
    c1 = '';

    file = open(path, encoding="utf8")
    # print(file.tell())

    line = ''
    while line[:2] != 'EF':
        line = file.readline()
        # getting AF value
        p = extract_attr(file, file.tell(), line, 'AF')
        if p[1] != '':
            af = p[1]
            file.seek(p[0])
        # getting TI value
        p1 = extract_attr(file, file.tell(), line, 'TI')
        if p1[1] != '':
            ti = p1[1]
            file.seek(p1[0])
        # getting SO value
        p2 = extract_attr(file, file.tell(), line, 'SO')
        if p2[1] != '':
            so = p2[1]
            file.seek(p2[0])
        # getting LA value
        p3 = extract_attr(file, file.tell(), line, 'LA')
        if p3[1] != '':
            la = p3[1]
            file.seek(p3[0])
        # getting DT value
        p4 = extract_attr(file, file.tell(), line, 'DT')
        if p4[1] != '':
            dt = p4[1]
            file.seek(p4[0])
        # getting DE value
        p5 = extract_attr(file, file.tell(), line, 'DE')
        if p5[1] != '':
            de = p5[1]
            file.seek(p5[0])
        # getting ID value
        p6 = extract_attr(file, file.tell(), line, 'ID')
        if p6[1] != '':
            id_ = p6[1]
            file.seek(p6[0])
        # getting AB value
        p7 = extract_attr(file, file.tell(), line, 'AB')
        if p7[1] != '':
            ab = p7[1]
            file.seek(p7[0])
        # getting NR value
        p8 = extract_attr(file, file.tell(), line, 'NR')
        if p8[1] != '':
            nr = p8[1]
            file.seek(p8[0])
        # getting TC value
        p9 = extract_attr(file, file.tell(), line, 'TC')
        if p9[1] != '':
            tc = p9[1]
            file.seek(p9[0])
        # getting U1 value
        p10 = extract_attr(file, file.tell(), line, 'U1')
        if p10[1] != '':
            u1 = p10[1]
            file.seek(p10[0])
        # getting U2 value
        p11 = extract_attr(file, file.tell(), line, 'U2')
        if p11[1] != '':
            u2 = p11[1]
            file.seek(p11[0])
        # getting PY value
        p12 = extract_attr(file, file.tell(), line, 'PY')
        if p12[1] != '':
            py = p12[1]
            file.seek(p12[0])
        # getting BP value
        p13 = extract_attr(file, file.tell(), line, 'BP')
        if p13[1] != '':
            bp = p13[1]
            file.seek(p13[0])
        # getting EP value
        p14 = extract_attr(file, file.tell(), line, 'EP')
        if p14[1] != '':
            ep = p14[1]
            file.seek(p14[0])
        # getting WC value
        p15 = extract_attr(file, file.tell(), line, 'WC')
        if p15[1] != '':
            wc = p15[1]
            file.seek(p15[0])
        # getting SC value
        p16 = extract_attr(file, file.tell(), line, 'SC')
        if p16[1] != '':
            sc = p16[1]
            file.seek(p16[0])
        # getting C1 value
        p17 = extract_attr(file, file.tell(), line, 'C1')
        if p17[1] != '':
            c1 = p17[1]
            file.seek(p17[0])

        if line[:2] == 'ER':
            record = {"AF": af.split('\n'),
                      "TI": ti,
                      "SO": so.split(';'),
                      "LA": la,
                      "DT": dt,
                      "DE": de.split(';'),
                      "ID": id_.split(';'),
                      "AB": ab,
                      "NR": nr,
                      "TC": tc,
                      "U1": u1,
                      "U2": u2,
                      "PY": py,
                      "BP": bp,
                      "EP": ep,
                      "WC": re.split('; |, ', wc),
                      "SC": sc.split(';'),
                      "C1": c1.split('\n'),
                      "FileName": path}
            # print(f"test=> {record['SC']}" )
            ExtractedAttrDicList.append(record)

    print(f"Extracted from {path}")

    return ExtractedAttrDicList
