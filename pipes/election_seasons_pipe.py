import os.path
import pandas as pd
import psycopg2
from lxml import etree


csv_path = 'data/preprocessed/election_seasons.csv'


def preprocess_data():
    with open(os.path.join("data", "raw", "kansanedustajat_vaalikausittain.csv"), "r") as f:
        mop = pd.read_csv(f, sep=",")

    seasons = mop.Vaalikausi.unique()
    seasons = [s.split("–") for s in seasons]
    seasons = [{"start_year": f"{s[0].split(" ")[0]}-01-01", "end_year": f"{s[1].split(" ")[0]}-12-31"} for s in seasons]

    seasons = pd.DataFrame(seasons)
    seasons.to_csv(csv_path, index=False)


def import_data():
    conn = psycopg2.connect(database="postgres",
                            host="db",
                            user="postgres",
                            password="postgres",
                            port="5432")
    cursor = conn.cursor()

    with open(csv_path) as f:
        cursor.copy_expert("COPY election_seasons(start_year, end_year) FROM stdin DELIMITERS ',' CSV HEADER QUOTE '\"';", f)

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
