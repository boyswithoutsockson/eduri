import os
import pandas as pd
import xmltodict
import psycopg2

csv_path = 'data/preprocessed/committees.csv'

def preprocess_data():
    committees = {"Suuri valiokunta": "SuV",
                    "Perustuslakivaliokunta": "PeV",
                    "Ulkoasiainvaliokunta": "UaV",
                    "Valtiovarainvaliokunta": "VaV",
                    "Tarkastusvaliokunta": "TrV",
                    "Hallintovaliokunta": "HaV",
                    "Lakivaliokunta": "LaV",
                    "Liikenne- ja viestintävaliokunta": "LiV",
                    "Maa- ja metsätalousvaliokunta": "MmV",
                    "Puolustusvaliokunta": "PuV",
                    "Sivistysvaliokunta": "SiV",
                    "Sosiaali- ja terveysvaliokunta": "StV",
                    "Talousvaliokunta": "TaV",
                    "Tiedusteluvalvontavaliokunta": "TiV",
                    "Tulevaisuusvaliokunta": "TuV",
                    "Työelämä- ja tasa-arvovaliokunta": "TyV",
                    "Ympäristövaliokunta": "YmV"}

    earliest_retirement_date = "2000-01-01"

    with open(os.path.join("data", "raw", "MemberOfParliament.tsv"), "r") as f:
        MoP = pd.read_csv(f, sep="\t")

    xml_dicts = MoP.XmlDataFi.apply(xmltodict.parse)
    committee_df = pd.DataFrame(columns=["committee_name"])
    
    for henkilo in xml_dicts:
        if henkilo['Henkilo']['KansanedustajuusPaattynytPvm']:
            retirement_date = "-".join(list(reversed((henkilo['Henkilo']['KansanedustajuusPaattynytPvm']).split("."))))
        else:
            continue

        if retirement_date < earliest_retirement_date:
            continue

        cur_committees = henkilo['Henkilo']['NykyisetToimielinjasenyydet']['Toimielin']

        if isinstance(cur_committees, dict):
            cur_committees = [cur_committees]
        
        try:
            prev_committees = henkilo['Henkilo']['AiemmatToimielinjasenyydet']['Toimielin']
        except TypeError:
            prev_committees = []

        if isinstance(prev_committees, dict):
            prev_committees = [prev_committees]

        for committee in cur_committees + prev_committees:
            if committee['Nimi']:
                if committee["@OnkoValiokunta"] == 'true':
                    committee_df.loc[len(committee_df)] = committee["Nimi"]
    
    committee_df = committee_df.drop_duplicates()
    committee_df.to_csv(csv_path, index=False)

def import_data():
    conn = psycopg2.connect(database="postgres",
                            host="db",
                            user="postgres",
                            password="postgres",
                            port="5432")
    cursor = conn.cursor()
    
    with open(csv_path) as f:
        cursor.copy_expert("COPY committees FROM stdin DELIMITERS ',' CSV HEADER QUOTE '\"';", f)
    
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