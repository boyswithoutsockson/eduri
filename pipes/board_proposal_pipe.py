import os.path
import csv
import psycopg2
import argparse
import xmltodict
import sys
import pandas as pd

csv.field_size_limit(sys.maxsize)
csv_path = 'data/preprocessed/board_proposal.csv'

def preprocess_data():
    with open(os.path.join("data", "raw", "VaskiData.tsv"), "r") as f:
       vaski = pd.read_csv(f, delimiter="\t", quotechar='"', nrows=10000)

    board_proposals = vaski[vaski["Eduskuntatunnus"].str.startswith("HE")]
    bps = {}

    for bp in board_proposals["XmlData"]:
        xml = xmltodict.parse(bp)
        try:
            if xml['ns11:Siirto']['Sanomavalitys']['ns4:SanomatyyppiNimi']['#text'].endswith('GovernmentProposal_fi'):
                bp_brief = xml['ns11:Siirto']['SiirtoAsiakirja']['RakenneAsiakirja']['HE:HallituksenEsitys']['asi:SisaltoKuvaus']
                bp_args = xml['ns11:Siirto']['SiirtoAsiakirja']['RakenneAsiakirja']['HE:HallituksenEsitys']['asi:PerusteluOsa']
        except KeyError:
            try:
                if xml['ns11:Siirto']['Sanomavalitys']['ns2:SanomatyyppiNimi']['#text'].endswith('GovernmentProposal_fi'):
                    pass
            except KeyError:
                import pdb;pdb.set_trace()
        bps.update({})

    with open(csv_path, 'w') as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writerows(rows)

def import_data():
    conn = psycopg2.connect(database="postgres",
                            host="db",
                            user="postgres",
                            password="postgres",
                            port="5432")
    cursor = conn.cursor()

    with open(csv_path) as f:
        cursor.copy_expert("COPY ballots(id, title, session_item_title, start_time, parliament_id, minutes_url, results_url) FROM stdin DELIMITERS ',' CSV QUOTE '\"';", f)
        
    conn.commit()
    cursor.close()
    conn.close()

if __name__ == '__main__':
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
