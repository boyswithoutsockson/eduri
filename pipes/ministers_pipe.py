import os
import pandas as pd
import xml.etree.ElementTree as ET
import psycopg2

from db import get_connection


csv_path = 'data/preprocessed/ministers.csv'
minister_position_csv_path = 'data/preprocessed/minister_positions.csv'

def preprocess_data():
    with open(os.path.join("data", "raw", "MemberOfParliament.tsv")) as f:
        df = pd.read_csv(f, sep="\t")

    rows = []
    minister_positions = []

    for mp in df.iterrows():
        mp = mp[1]

        person_id = mp['personId']
        xml = mp['XmlDataFi']
        tree = ET.fromstring(xml)

        for jasenyys in tree.findall(".//ValtioneuvostonJasenyydet/Jasenyys"):
            ministry = jasenyys.findtext("Ministeriys")
            
            if ministry:  # Only include if minister position exists
                minister_position = jasenyys.findtext("Nimi")
                cab_id = jasenyys.findtext("Hallitus")
                start = jasenyys.findtext("AlkuPvm")
                end = jasenyys.findtext("LoppuPvm")
                
                # Alter date format
                start = "-".join(reversed(start.split(".")))
                end = "-".join(reversed(end.split(".")))
                if "-" not in start:
                    start = start + "-01-01"
                    end = end + "-12-31"
                
                rows.append({
                    "person_id": person_id,
                    "minister_position": minister_position,
                    "cabinet_id": cab_id,
                    "start_date": start,
                    "end_date": end
                })
                minister_positions.append(minister_position)

    minister_position_df = pd.DataFrame(list(set(minister_positions)))
    minister_position_df.to_csv(minister_position_csv_path, index=False, header=False)
    df = pd.DataFrame(rows)
    df.to_csv(csv_path, index=False, header=False)
    

def import_data():
    conn = get_connection()
    cursor = conn.cursor()

    with open(minister_position_csv_path) as f:
        cursor.copy_expert("COPY minister_positions(title) FROM stdin DELIMITERS ',' CSV QUOTE '\"';", f)

    with open(csv_path) as f:
        cursor.copy_expert("COPY ministers(person_id, minister_position, cabinet_id, start_date, end_date) FROM stdin DELIMITERS ',' CSV QUOTE '\"';", f)

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
        

