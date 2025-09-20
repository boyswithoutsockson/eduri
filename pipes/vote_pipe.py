import os.path
import csv
import psycopg2
import pandas as pd

csv_path = 'data/preprocessed/votes.csv'

vote_dict = {"Jaa": "yes",
            "Ei": "no",
            "Poissa": "absent",
            "Tyhjää": "abstain"}

def preprocess_data():
    with open(os.path.join("data", "raw", "SaliDBAanestysEdustaja.tsv")) as f:
        vote_data = pd.read_csv(f, sep="\t")[["EdustajaHenkiloNumero", "AanestysId", "EdustajaAanestys"]]

    vote_data = vote_data[
        vote_data["EdustajaAanestys"].str.strip().isin(vote_dict.keys())
    ]
    vote_data["EdustajaAanestys"] = vote_data["EdustajaAanestys"].str.strip().apply(
        lambda x: vote_dict[x]
    )

    vote_data.columns = ["person_id", "ballot_id", "vote"]
    vote_data.to_csv(csv_path, index=False)

def import_data():
    conn = psycopg2.connect(database="postgres",
                            host="db",
                            user="postgres",
                            password="postgres",
                            port="5432")
    cursor = conn.cursor()
    with open(csv_path) as f:
        cursor.execute("ALTER TABLE votes DISABLE TRIGGER ALL;")
        cursor.copy_expert("COPY votes(person_id, ballot_id, vote) FROM stdin DELIMITERS ',' CSV HEADER QUOTE '\"';", f)
        cursor.execute("ALTER TABLE votes ENABLE TRIGGER ALL;")
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
