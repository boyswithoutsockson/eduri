import os.path
import csv
import argparse

from db import get_connection, bulk_insert

csv_path = "data/preprocessed/ballots.csv"


def preprocess_data():
    with open(os.path.join("data", "raw", "SaliDBAanestys.tsv")) as f:
        ballot_data = list(csv.reader(f, delimiter="\t", quotechar='"'))

    rows = []
    for ballot in ballot_data[1:]:
        if ballot[1] == "1":  # KieliId == 1
            row = {
                "id": ballot[0],
                "title": ballot[12],
                "session_item_title": ballot[21],
                "start_time": f"{ballot[9]} Europe/Helsinki",
                "parliament_id": ballot[31].lower(),
                "minutes_url": ballot[30],
                "results_url": ballot[28],
            }
            rows.append(row)

    with open(csv_path, "w") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writerows(rows)


def import_data():
    conn = get_connection()
    cursor = conn.cursor()

    with open(csv_path) as f:
        bulk_insert(
            cursor,
            "ballots",
            [
                "id",
                "title",
                "session_item_title",
                "start_time",
                "parliament_id",
                "minutes_url",
                "results_url",
            ],
            f,
        )

    conn.commit()
    cursor.close()
    conn.close()


if __name__ == "__main__":
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
