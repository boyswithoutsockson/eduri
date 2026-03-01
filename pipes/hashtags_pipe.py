import os
import pandas as pd
from db import get_connection


hashtags_tsv_path = os.path.join("data", "preprocessed", "hashtags.csv")
proposal_hashtags_tsv_path = os.path.join(
    "data", "preprocessed", "proposal_hashtags.csv"
)


def write_hashtags(hashtags):
    pd.DataFrame(hashtags).to_csv(
        proposal_hashtags_tsv_path,
        index=False,
        encoding="utf-8",
        mode="a",
        header=False,
    )


def preprocess_data():
    # add header row
    proposal_hashtags = pd.read_csv(
        proposal_hashtags_tsv_path, names=["proposal_id", "hashtag"]
    )
    proposal_hashtags.to_csv(proposal_hashtags_tsv_path, index=False, encoding="utf-8")
    (
        proposal_hashtags["hashtag"]
        .drop_duplicates()
        .to_csv(hashtags_tsv_path, index=False, encoding="utf-8")
    )


def import_data():
    conn = get_connection()
    cur = conn.cursor()

    with open(hashtags_tsv_path, "r", encoding="utf-8") as f:
        cur.copy_expert(
            """
            COPY hashtags(hashtag)
            FROM STDIN WITH (FORMAT CSV, HEADER TRUE, QUOTE '\"');
            """,
            f,
        )

    with open(proposal_hashtags_tsv_path, "r", encoding="utf-8") as f:
        cur.copy_expert(
            """
            COPY proposal_hashtags(proposal_id, hashtag)
            FROM STDIN WITH (FORMAT CSV, HEADER TRUE, QUOTE '\"');
            """,
            f,
        )

    conn.commit()
    cur.close()
    conn.close()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--preprocess-data", action="store_true", help="Parse TSV and write CSVs"
    )
    parser.add_argument(
        "--import-data", action="store_true", help="Import CSVs into Postgres"
    )
    args = parser.parse_args()

    if args.preprocess_data:
        preprocess_data()
    if args.import_data:
        import_data()
    if not args.preprocess_data and not args.import_data:
        preprocess_data()
        import_data()
