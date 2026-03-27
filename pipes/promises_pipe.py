import os
import polars as pl

from db import get_connection, bulk_insert

json_path = os.path.join("data", "raw", "promises_2023.json")
csv_path = os.path.join("data", "preprocessed", "promises.csv")


def preprocess_data():
    # ELECTION 2023 ONLY
    promises_df = pl.read_json(json_path, infer_schema_length=10000)

    promises_df = promises_df.rename(
        {"firstName": "first_name", "lastName": "last_name"}
    )

    # Remove the nickname of Ritva "Kike" Elomaa
    promises_df = promises_df.with_columns(
        pl.col("first_name").replace('Ritva "Kike"', "Ritva")
    )

    conn = get_connection()
    cursor = conn.cursor()

    # Fetch active mps
    cursor.execute("""SELECT DISTINCT
                        person_id,
                        first_name,
                        last_name
                    FROM (mp_parliamentary_group_memberships
                    INNER JOIN persons ON persons.id = person_id)
                    WHERE end_date is null 
                    ;""")

    mp_df = pl.DataFrame(
        cursor.fetchall(), schema=["person_id", "first_name", "last_name"], orient="row"
    )

    cursor.close()
    conn.close()

    # Cast types to enable join
    mp_df = mp_df.with_columns(
        pl.col("first_name").cast(pl.Utf8), pl.col("last_name").cast(pl.Utf8)
    )

    promises_df = promises_df.join(mp_df, on=["first_name", "last_name"], how="inner")

    # Add election year
    promises_df = promises_df.with_columns(pl.lit(2023).alias("election_year"))

    # Select correct rows in correct order
    promises_df = promises_df.select(
        pl.col("person_id"), pl.col("promise"), pl.col("election_year")
    )

    promises_df.write_csv(csv_path)


def import_data():
    conn = get_connection()
    cursor = conn.cursor()

    with open(csv_path) as f:
        bulk_insert(
            cursor,
            "promises",
            ["person_id", "promise", "election_year"],
            f,
            has_header=True,
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
