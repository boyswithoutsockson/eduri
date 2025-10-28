import os
import polars as pl
from matching_help_functions import match_target_mp

from db import get_connection

json_path = os.path.join("data", "raw", "lobby_actions.json")
topics_csv_path = 'data/preprocessed/lobby_topics.csv'
actions_csv_path = 'data/preprocessed/lobby_actions.csv'

def preprocess_data():

    df = pl.read_json(json_path, infer_schema_length=10000)
    
    df = df.explode("topics")
    
    df = df.filter(pl.col("topics").is_not_null())  # Drop lobbies that have not lobbied yet, only registered 
    
    df = df.select(
        pl.col("companyId"),
        pl.col("otherCompanyId"),
        pl.col("termId"),
        pl.col("topics").struct.field("contactTopicOther"),
        pl.col("topics").struct.field("id").alias("topicId"),
        pl.col("topics").struct.field("contactedTargets"),
        pl.col("topics").struct.field("contactTopicProject"),
        )
    
    # Opening nested parts of the json and dropping useless columns
    df = df.unnest("contactTopicProject").drop(["en", "sv", "createdAt", "id"])
 
    df = df.explode("contactedTargets").unnest("contactedTargets").explode("contactMethods").drop("activityNotificationTopicId")
    
    df = df.with_columns(
        pl.coalesce([pl.col("companyId"), pl.col("otherCompanyId")]).alias("lobby_id")).drop(["companyId", "otherCompanyId"])
    
    df = df.with_columns(
        pl.coalesce([pl.col("fi"), pl.col("contactTopicOther")]).alias("topic")).drop(["contactTopicOther", "fi"])

    # Parsing topics
    topics_df = df.select(pl.col(["topicId", "projectId", "topic"]))
    
    topics_df = topics_df.select(
        pl.col("topicId").alias("id"),
        pl.col("topic"),
        pl.col("projectId").alias("project")
        ).unique()
    
    topics_df.write_csv(topics_csv_path)
    
    # Parsing actions
    persons_df = match_target_mp(df["contactedTargetId"]) # Matching targets to persons in database (only mps for now)

    actions_df = df.join(
        persons_df,
        right_on="target_id", 
        left_on="contactedTargetId",           
        how="inner"
        )
    
    actions_df = actions_df.select(pl.col("lobby_id"),
                                    pl.col("termId").alias("term_id"),
                                    pl.col("person_id"),
                                    pl.col("topicId").alias("topic_id"),
                                    pl.col("contactMethods").alias("contact_method"))

    actions_df.write_csv(actions_csv_path)

def import_data():
    conn = get_connection()
    cursor = conn.cursor()
    
    with open(topics_csv_path) as f:
        cursor.copy_expert("COPY lobby_topics(id, topic, project) FROM stdin DELIMITERS ',' CSV HEADER QUOTE '\"';", f)

    with open(actions_csv_path) as f:
        cursor.copy_expert("COPY lobby_actions(lobby_id, term_id, person_id, topic_id, contact_method) FROM stdin DELIMITERS ',' CSV HEADER QUOTE '\"';", f)
    
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