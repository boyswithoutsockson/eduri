import os
import polars as pl

from db import get_connection

raw_path = os.path.join("data", "raw", "election23_fundings.csv")
csv_path = os.path.join("data", "preprocessed", "election_fundings.csv")

# Map to translate the VTV codes to eduri database funding types
funding_type_map = {
                        "2.2": 'loan', 
                        "2.3": 'person', 
                        "2.4": 'company', 
                        "2.5": 'party', 
                        "2.6": 'party_union', 
                        "2.7": 'other', 
                        "2.8": 'forwarded'
                    }

def preprocess_data():

    fundings_df = pl.read_csv(raw_path, infer_schema_length=10000, separator=";")

    fundings_df = fundings_df.with_columns(pl.lit(2023).alias("election_year"),
                         pl.col("'Tukilahteen lomakenumerot'").str.strip_chars("'").replace(funding_type_map).alias("ftype"),
                         pl.col("'Tukija'").str.strip_chars("'").alias("funder_organization"),
                         pl.col("'Tukijan y-tunnus/yhdistysrekisterinumero jos on'").str.strip_chars("'").alias("funder_company_id"),
                         pl.col("'Tukijan etunimet'").str.strip_chars("'").alias("funder_first_name"),
                         pl.col("'Tukijan sukunimi'").str.strip_chars("'").alias("funder_last_name"),
                         pl.col("'Tuen saajan etunimet'").str.strip_chars("'").alias("first_name"),
                         pl.col("'Tuen saajan Sukunimi'").str.strip_chars("'").alias("last_name"),
                         pl.col("'Lainan nimi'").str.strip_chars("'").alias("loan_title"),
                         pl.col("'Lainan takaisinmaksuaika/laina-aika'").str.strip_chars("'").alias("loan_schedule"),
                         pl.col("'Tuen maara'").str.strip_chars("'").str.replace_all(r"\s", "").str.replace(",", ".").cast(pl.Float32).alias("amount"))
    
    # Drop original columns
    fundings_df = fundings_df[:, -11:]
    
    # Fetch active mps
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""SELECT DISTINCT
                        person_id,
                        first_name,
                        last_name
                    FROM (public.mp_parliamentary_group_memberships
                    INNER JOIN public.persons ON persons.id = person_id)
                    WHERE end_date is null 
                    ;""")

    mp_df = pl.DataFrame(cursor.fetchall(), schema=["person_id", "first_name", "last_name"], orient="row")

    cursor.close()
    conn.close()

    # Cast types to enable join
    mp_df = mp_df.with_columns(
                pl.col("first_name").cast(pl.Utf8),
                pl.col("last_name").cast(pl.Utf8))

    # Join to match mps to their ids
    fundings_df = fundings_df.join(mp_df,
                on=["first_name", "last_name"],
                how="inner")
    
    # Drop useless mp name columns
    fundings_df = fundings_df.drop(["first_name", "last_name"])

    # Reorder
    fundings_df = fundings_df.select([fundings_df.columns[-1], *fundings_df.columns[:-1]])

    fundings_df.write_csv(csv_path)

def import_data():
    conn = get_connection()
    cursor = conn.cursor()
    
    with open(csv_path) as f:
        cursor.copy_expert("COPY election_fundings(person_id, election_year, ftype, funder_organization, funder_company_id, funder_first_name, " \
                                        "funder_last_name, loan_title, loan_schedule, amount) FROM stdin DELIMITERS ',' CSV HEADER QUOTE '\"';", f)
    
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