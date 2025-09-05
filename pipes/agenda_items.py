import os.path
import pandas as pd
import numpy as np
import xmltodict
import psycopg2
from harmonize import harmonize_party

csv_path = 'data/preprocessed/agenda_items.csv'


def preprocess_data():
    with open(os.path.join("data", "raw", "SaliDBKohta.tsv"), "r") as f:
        AI = pd.read_csv(f, sep="\t")

    # Jos suomenkielistä otsikkoa ei ole, käytetään ruotsinkielistä
    AI.OtsikkoFI = AI.OtsikkoFI.combine_first(AI.OtsikkoSV)

    AI = AI[["Id", "PJKohtaTunnus", "OtsikkoFI"]]
    AI.columns = ["id", "parliament_id", "title"]

    AI.to_csv(csv_path, index=False)


def import_data():
    conn = psycopg2.connect(database="postgres",
                            host="db",
                            user="postgres",
                            password="postgres",
                            port="5432")
    cursor = conn.cursor()

    with open(csv_path) as f:
        cursor.copy_expert("COPY agenda_items(id, parliament_id, title) FROM stdin DELIMITERS ',' CSV HEADER QUOTE '\"';", f)

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
