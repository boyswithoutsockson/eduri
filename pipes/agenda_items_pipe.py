import os.path
import pandas as pd
import psycopg2

from db import get_connection

csv_path = 'data/preprocessed/agenda_items.csv'


def preprocess_data():
    with open(os.path.join("data", "raw", "SaliDBKohta.tsv"), "r") as f:
        AI = pd.read_csv(f, sep="\t")

    # Jos suomenkielistä otsikkoa ei ole, käytetään ruotsinkielistä
    AI.OtsikkoFI = AI.OtsikkoFI.combine_first(AI.OtsikkoSV)

    AI = AI[["Id", "PJKohtaTunnus", "IstuntoTekninenAvain", "OtsikkoFI"]]
    AI.columns = ["id", "parliament_id", "session_id", "title"]

    AI.to_csv(csv_path, index=False)


def import_data():
    conn = get_connection()
    cursor = conn.cursor()

    with open(csv_path) as f:
        cursor.copy_expert("COPY agenda_items(id, parliament_id, session_id, title) FROM stdin DELIMITERS ',' CSV HEADER QUOTE '\"';", f)

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
