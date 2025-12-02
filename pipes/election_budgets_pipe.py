import os
import polars as pl

from db import get_connection

raw_path = os.path.join("data", "raw", "election23_budgets.csv")
csv_path = os.path.join("data", "preprocessed", "election_budgets.csv")

def preprocess_data():

    budgets_df = pl.read_csv(raw_path, infer_schema_length=10000, separator=";")

    budgets_df = budgets_df.with_columns(
                        pl.col("'Etunimet'").str.strip_chars("'").alias("first_name"),
                        pl.col("'Sukunimi'").str.strip_chars("'").alias("last_name"),
                        pl.col("'Ehdokkaan mahdollisen tukiryhman nimi'").str.strip_chars("'").alias("support_group"),
                        pl.lit(2023).alias("election_year"),
                        pl.col("'Vaalikampanjan kulut yhteensa'").str.strip_chars("'").str.replace_all(r"\s", "").replace("", "0").str.replace(",", ".").cast(pl.Float32).alias("expenses_total"),
                        pl.col("'Vaalikampanjan rahoitus yhteensa'").str.strip_chars("'").str.replace_all(r"\s", "").replace("", "0").str.replace(",", ".").cast(pl.Float32).alias("incomes_total"),
                        pl.col("'2.1 Rahoitus sisaltaa omia varoja yhteensa'").str.strip_chars("'").str.replace_all(r"\s", "").replace("", "0").str.replace(",", ".").cast(pl.Float32).alias("income_own"),
                        pl.col("'2.2 Rahoitus sisaltaa ehdokkaan ja tukiryhman ottamia lainoja yhteensa'").str.strip_chars("'").str.replace_all(r"\s", "").replace("", "0").str.replace(",", ".").cast(pl.Float32).alias("income_loan"),
                        pl.col("'2.3 Rahoitus sisaltaa yksityishenkiloilta saatua tukea yhteensa'").str.strip_chars("'").str.replace_all(r"\s", "").replace("", "0").str.replace(",", ".").cast(pl.Float32).alias("income_person"),
                        pl.col("'2.4 Rahoitus sisaltaa yrityksilta saatua tukea yhteensa'").str.strip_chars("'").str.replace_all(r"\s", "").replace("", "0").str.replace(",", ".").cast(pl.Float32).alias("income_company"),
                        pl.col("'2.5 Rahoitus sisaltaa puolueelta saatua tukea yhteensa'").str.strip_chars("'").str.replace_all(r"\s", "").replace("", "0").str.replace(",", ".").cast(pl.Float32).alias("income_party"),
                        pl.col("'2.6 Rahoitus sisaltaa puolueyhdistyksilta saatua tukea yhteensa'").str.strip_chars("'").str.replace_all(r"\s", "").replace("", "0").str.replace(",", ".").cast(pl.Float32).alias("income_party_union"),
                        pl.col("'2.8 Rahoitus sisaltaa valitettya tukea yhteensa'").str.strip_chars("'").str.replace_all(r"\s", "").replace("", "0").str.replace(",", ".").cast(pl.Float32).alias("income_forwarded"),
                        pl.col("'2.7 Rahoitus sisaltaa muilta tahoilta saatua tukea yhteensa'").str.strip_chars("'").str.replace_all(r"\s", "").replace("", "0").str.replace(",", ".").cast(pl.Float32).alias("income_other"))
    
    # Drop original columns
    budgets_df = budgets_df[:, -14:]
    
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
    budgets_df = budgets_df.join(mp_df,
                on=["first_name", "last_name"],
                how="inner")
    
    # Drop useless mp name columns
    budgets_df = budgets_df.drop(["first_name", "last_name"])

    # Reorder
    budgets_df = budgets_df.select([budgets_df.columns[-1], *budgets_df.columns[:-1]])

    budgets_df.write_csv(csv_path)

def import_data():
    conn = get_connection()
    cursor = conn.cursor()
    
    with open(csv_path) as f:
        cursor.copy_expert("COPY election_budgets(person_id, support_group, election_year, expenses_total, incomes_total, income_own, income_loan, income_person, income_company, " \
                        "income_party, income_party_union, income_forwarded, income_other) FROM stdin DELIMITERS ',' CSV HEADER QUOTE '\"';", f)
    
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