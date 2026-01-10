import os
import polars as pl

from db import get_connection

json_path = os.path.join("data", "raw", "finto_topics.json")
csv_path = "data/preprocessed/topics.csv"


def preprocess_data():
    df = pl.read_json(json_path, infer_schema_length=10000)

    df.write_csv(csv_path)


def import_data():
    conn = get_connection()
    cursor = conn.cursor()

    with open(csv_path) as f:
        cursor.copy_expert(
            "COPY topics(topic_id, term) FROM stdin DELIMITERS ',' CSV HEADER QUOTE '\"';",
            f,
        )

    conn.commit()
    cursor.close()
    conn.close()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--preprocess-data", help="preprocess the data", action="store_true"
    )
    parser.add_argument(
        "--import-data", help="import preprocessed data", action="store_true"
    )
    args = parser.parse_args()
    if args.preprocess_data:
        preprocess_data()
    if args.import_data:
        import_data()
    if not args.preprocess_data and not args.import_data:
        preprocess_data()
        import_data()
