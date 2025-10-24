import os.path
import pandas as pd
import psycopg2
from lxml import etree

from db import get_connection


csv_path = 'data/preprocessed/election_seasons.csv'


def preprocess_data():
    with open(os.path.join("data", "raw", "election_seasons.tsv"), "r") as f:
        seasons = pd.read_csv(f, sep="\t")

    seasons.rename(columns={'alkupvm': 'start_date', 'loppupvm': 'end_date'}, inplace=True)
    seasons = seasons.drop(['nimi', 'jarjestys', 'aktiivinen', 'tunnus'], axis=1)
    seasons.to_csv(csv_path, index=False)

def import_data():
    conn = get_connection()
    cursor = conn.cursor()

    with open(csv_path) as f:
        cursor.copy_expert("COPY election_seasons(start_date, end_date) FROM stdin DELIMITERS ',' CSV HEADER QUOTE '\"';", f)

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
