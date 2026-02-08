import os
import polars as pl
from db import get_connection

json_path = os.path.join("data", "raw", "lobby_targets.json")


def match_target_mp(target_ids):
    # Creating a dataframe for all targets

    targets_df = pl.read_json(json_path).drop(
        ["createdAt", "id", "fiId", "svId", "enId", "termId", "hash", "sv", "en"]
    )
    targets_df = targets_df.unnest("fi").drop("createdAt")

    # "name" column holds the name of the person contacted.
    # If there is no name, the target is an organization.
    targets_df = targets_df.filter([pl.col("name") != "-", pl.col("name") != ""])

    targets_df = targets_df.with_columns(
        pl.col("name").str.split(by=" ").list.get(0).alias("target_first_name"),
        pl.col("name").str.split(by=" ").list.get(1).alias("target_last_name"),
    )

    targets_df = targets_df.with_columns(pl.col("id").alias("target_id"))

    # Fetching all persons from database

    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""SELECT DISTINCT
                        person_id,
                        first_name,
                        last_name
                    FROM (mp_parliamentary_group_memberships
                    INNER JOIN persons ON persons.id = person_id)
                    WHERE end_date is null OR end_date > '2024-01-01'    
                    ;""")  # Filter out people who have retired from the parliament before the implementation of avoimuusrekisteri

    df = pl.DataFrame(
        cursor.fetchall(), schema=["person_id", "first_name", "last_name"], orient="row"
    )

    joined_df = df.join(  # Joining the dataframes by first_name + last_name
        targets_df,
        left_on=["first_name", "last_name"],
        right_on=["target_first_name", "target_last_name"],
        how="inner",
    ).drop("name")

    matches_df = joined_df.filter(
        pl.col("target_id").is_in(target_ids)
    )  # Only pick the ones in the list of wanted targets
    matches_df = matches_df.select(pl.col("person_id"), pl.col("id").alias("target_id"))

    return matches_df
