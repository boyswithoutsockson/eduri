import os.path
import polars as pl
from lxml import etree
from io import StringIO
from XML_parsing_help_functions import absentee_parse
import re

from db import get_connection

absences_csv_path = os.path.join("data", "preprocessed", "absences.csv")


def preprocess_data():

    # Create master dataframe for all absence instances
    absences_df = pl.DataFrame(schema={
                                "person_id": pl.Int64,
                                "record_assembly_code": pl.Utf8,
                                "record_number": pl.Int32,
                                "record_year": pl.Int32,
                                "work_related": pl.Boolean
                                })

    conn = get_connection()
    cursor = conn.cursor()

    # Fetch records for parliament plenary sessions (eduskunnan istuntojen pöytäkirjat)
    cursor.execute("""SELECT DISTINCT
                        assembly_code,
                        number,
                        year,
                        rollcall_id
                    FROM records
                    WHERE assembly_code = 'EK'  
                    ;""")
    
    records_df = pl.DataFrame(cursor.fetchall(), schema=["assembly_code", "number", "year", "rollcall_id"], orient="row")

    # Load the TSV file for rollcall reports

    df_tsv = pl.read_csv(os.path.join("data", "raw", "vaski", "RollCallReport_fi.tsv"), separator="\t")

    # Iterate over rollcall reports. In practice, iterates over meetings.
    for xml_str, id in zip(df_tsv['XmlData'], df_tsv['Eduskuntatunnus']):

        # Two anomalies in the data
        if id in ["EDK-2016-AK-99126", "00000000-0000-0000-0000-000000000000"]: 
            continue

        root = etree.parse(StringIO(xml_str)).getroot()

        # Fetch list of people absent from this meeting
        absentees_df = pl.DataFrame(absentee_parse(root))
        if absentees_df.is_empty():
            continue
        
        # The regular format for the id is "EDK-2016-AK-99126" 
        # There are a few instances in 2015 where the id is in format "PTK 1/2015 vp"
        # in these cases we can extract the record number and year from the id itself
        if id.startswith("PTK"):    
            assembly_code = "EK"    # Set the assembly code manually to "EK", which stands for the parliament (eduskunta)
            number, year = re.findall(r"\d+", id)
        else:
            # Else, search assembly code, number and year from the record table
            record = records_df.filter(pl.col("rollcall_id") == id).rows()
            # In case the corresponding report could not be found, continue
            # New rollcalls are often published before their corresponding report
            if record:
                assembly_code, number, year = record[0][:3]
            else:
                print(f"Could not find the corresponding report for rollcall {id}")
                continue

        # Attach the meeting specs to the list of absentees
        absentees_df = absentees_df.with_columns(
                        pl.lit(assembly_code).alias("record_assembly_code").cast(pl.Utf8),
                        pl.lit(number).alias("record_number").cast(pl.Int32),
                        pl.lit(year).alias("record_year").cast(pl.Int32)
                        )
        
        # Reorder for clarity
        absentees_df = absentees_df.select(
                        "person_id",
                        "record_assembly_code",
                        "record_number",
                        "record_year",
                        "work_related")           
        
        # Add the absentees of this meeting to the master dataframe
        absences_df.extend(absentees_df)

    absences_df.write_csv(absences_csv_path)
    
def import_data():
    conn = get_connection()
    cursor = conn.cursor()
    
    with open(absences_csv_path) as f:
        cursor.copy_expert("COPY absences(person_id, record_assembly_code, record_number, record_year, work_related) FROM stdin DELIMITERS ',' CSV HEADER QUOTE '\"';", f)
    
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
