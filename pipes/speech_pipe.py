import os.path
import csv
import psycopg2
from lxml import etree
import pandas as pd
import xmltodict


def preprocess_data():
    with open(os.path.join("data", "raw", "VaskiData.tsv")) as f:
        vaski_data = pd.read_csv(f, sep="\t", nrows=2000)["XmlData"]

    xmls = vaski_data.apply(xmltodict.parse)
    for xml in xmls:
        try:
            print(xml["ns11:Siirto"]["Sanomavalitys"]["ns4:SanomatyyppiNimi"]["#text"])
        except:
            continue

preprocess_data()