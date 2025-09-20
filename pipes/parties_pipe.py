import os.path
import pandas as pd
import numpy as np
import xmltodict
import psycopg2
from harmonize import harmonize_parliamentary_group

csv_path = 'data/preprocessed/parliamentary_groups.csv'


def preprocess_data():
    with open(os.path.join("data", "raw", "MemberOfParliament.tsv"), "r") as f:
        MoP = pd.read_csv(f, sep="\t")

    xml_dicts = MoP.XmlDataFi.apply(xmltodict.parse)
    parliamentary_groups = []
    for henkilo in xml_dicts:
        try:
            parliamentary_groups.append(henkilo['Henkilo']['Eduskuntaryhmat']['NykyinenEduskuntaryhma']['Nimi'])
        except KeyError:
            pass  # Ei nykyistä puoluetta
        try:
            parliamentary_groups.append(henkilo['Henkilo']['Eduskuntaryhmat']['EdellisetEduskuntaryhmat']['Eduskuntaryhma']['Nimi'])
        except TypeError:
            if henkilo['Henkilo']['Eduskuntaryhmat']['EdellisetEduskuntaryhmat']:
                for ekr in henkilo['Henkilo']['Eduskuntaryhmat']['EdellisetEduskuntaryhmat']['Eduskuntaryhma']:
                    parliamentary_groups.append(ekr['Nimi'])
    parliamentary_groups = [p for p in set(parliamentary_groups) if p is not None]

    parliamentary_group_keys = [harmonize_parliamentary_group(parliamentary_group) for parliamentary_group in parliamentary_groups]
    parliamentary_groups_df = pd.DataFrame({"id": parliamentary_group_keys, "name": parliamentary_groups})

    parliamentary_groups_df.to_csv(csv_path, index=False)


def import_data():
    conn = psycopg2.connect(database="postgres",
                            host="db",
                            user="postgres",
                            password="postgres",
                            port="5432")
    cursor = conn.cursor()

    with open(csv_path) as f:
        cursor.copy_expert("COPY parliamentary_groups(id, name) FROM stdin DELIMITERS ',' CSV HEADER QUOTE '\"';", f)

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
