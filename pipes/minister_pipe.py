import os
import pandas as pd
import xml.etree.ElementTree as ET
import csv
import psycopg2


csv_path = 'data/preprocessed/ministers.csv'


def preprocess_data():
    with open(os.path.join("data", "raw", "MemberOfParliament.tsv")) as f:
        df = pd.read_csv(f, sep="\t")

    rows = []
    for mp in df.iterrows():
        mp = mp[1]

        person_id = mp['personId']
        xml = mp['XmlDataFi']
        tree = ET.fromstring(xml)

        for jasenyys in tree.findall(".//ValtioneuvostonJasenyydet/Jasenyys"):
            ministry = jasenyys.findtext("Ministeriys")
            
            if ministry:  # Only include if minister position exists
                ministry_name = jasenyys.findtext("Nimi")
                cab_id = jasenyys.findtext("Hallitus")
                start = jasenyys.findtext("AlkuPvm")
                end = jasenyys.findtext("LoppuPvm")

                # Alter date format
                start = "-".join(reversed(start.split(".")))
                end = "-".join(reversed(start.split(".")))
                if "-" not in start:
                    start = start + "-01-01"
                    end = end + "-12-31"
            
                rows.append({
                    "person_id": person_id,
                    "ministry": ministry_name,
                    "cabinet_id": cab_id,
                    "start_date": start,
                    "end_date": end
                })

    df = pd.DataFrame(rows)
    df.to_csv(csv_path, index=False, header=False)

        
def import_data():
    conn = psycopg2.connect(database="postgres",
                            host="db",
                            user="postgres",
                            password="postgres",
                            port="5432")
    cursor = conn.cursor()

    with open(csv_path) as f:
        cursor.copy_expert("COPY ministers(person_id, ministry, cabinet_id, start_date, end_date) FROM stdin DELIMITERS ',' CSV QUOTE '\"';", f)

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
        

