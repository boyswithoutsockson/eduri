import os.path
import xml.etree.ElementTree as ET
import csv
import psycopg2
import xmltodict
import pandas as pd

csv_path = 'data/preprocessed/interests.csv'

def preprocess_data():
    with open(os.path.join("data", "raw", "MemberOfParliament.tsv"), "r") as f:
        MoP = pd.read_csv(f, sep="\t")

    xml_dicts = MoP.XmlDataFi.apply(xmltodict.parse)
    rows = []
    for henkilo in xml_dicts:
        mp_id = henkilo['Henkilo']['HenkiloNro']

        if henkilo['Henkilo']['Sidonnaisuudet'] is None:
            continue

        interest = henkilo['Henkilo']['Sidonnaisuudet']['Sidonnaisuus']

        if isinstance(interest, dict):
            interest = [interest]

        rows.extend([
            {'mp_id': mp_id, 'category': x['RyhmaOtsikko'], 'interest': x['Sidonta']}
            for x in interest
            if 'Sidonta' in x and x['Sidonta'] not in [None, 'Ei ilmoitettavia sidonnaisuuksia', 'Ei ilmoitettavia tuloja']
        ])

    with open('data/interests.csv', 'w') as f:
        writer = csv.DictWriter(f, fieldnames=["mp_id", "category", "interest"])
        writer.writerows(rows)

def import_data():
    conn = psycopg2.connect(database="postgres",
                            host="db",
                            user="postgres",
                            password="postgres",
                            port="5432")
    cursor = conn.cursor()

    with open(csv_path) as f:
        cursor.copy_expert("COPY interests(mp_id, category, interest) FROM stdin DELIMITERS ',' CSV QUOTE '\"';", f)

    conn.commit()
    cursor.close()
    conn.close()

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--preprocess-data", help="preprocess the data", action="store_true")
    parser.add_argument("--import-data", help="import preprocessed data", action="store_true")
    args = parser.parse_args()
    if args.preprocess_data:
        preprocess_data()
    if args.import_data:
        import_data()
    if not args.preprocess_data and not args.import_data:
        preprocess_data()
        import_data()