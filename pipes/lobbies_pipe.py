import os
import polars as pl

from db import get_connection, bulk_insert

json_path = os.path.join("data", "raw", "lobby_actions.json")
csv_path = "data/preprocessed/lobbies.csv"


def preprocess_data():
    lobbies_df = pl.read_json(json_path, infer_schema_length=10000)

    lobbies_df = lobbies_df.with_columns(
        pl.when(pl.col("companyId").is_not_null())  # Lobbies have a "companyId"
        .then(pl.col("companyId"))  # have a standard finnish Y-tunnus
        .otherwise(
            pl.col("otherCompanyId")
        )  # others have their own id under "otherCompanyId"
        .alias("lobby_id")
    )

    lobbies_df = lobbies_df.with_columns(
        [
            (pl.col("companyName").alias("name")),
            pl.col("mainIndustry").alias("industry"),
        ]
    )

    lobbies_df = lobbies_df.select(["lobby_id", "name", "industry"])

    # The same company might not have their industry written down on every instance.
    # Here we make sure to pick an instance with an industry, if there ever was one.
    lobbies_df = (
        lobbies_df.sort("industry", nulls_last=True).group_by("lobby_id").first()
    )

    lobbies_df.write_csv(csv_path)


def import_data():
    conn = get_connection()
    cursor = conn.cursor()

    with open(csv_path) as f:
        bulk_insert(
            cursor, "lobbies", ["id", "name", "industry"], f, has_header=True
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
