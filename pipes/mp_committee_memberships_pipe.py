import os.path
import pandas as pd
import numpy as np
import argparse
import os
from lxml import etree
from io import StringIO
from XML_parsing_help_functions import id_parse, date_parse, Nimeke_parse, AsiaSisaltoKuvaus_parse, Perustelu_parse, Saados_parse, status_parse, Allekirjoittaja_parse
from db import get_connection

from db import get_connection

csv_path = os.path.join("data", "preprocessed", "mp_committee_memberships.csv")

def preprocess_data():

    mcm_records = []

    roles = {
        "jäsen": "member",
        "varajäsen": "associate",
        "lisäjäsen": "additional",
        "puheenjohtaja": "chair",
        "varapuheenjohtaja": "first vice",
        "ensimmäinen varapuheenjohtaja": "first vice",
        "toinen varapuheenjohtaja": "second vice",
    }

    with open(os.path.join("data", "raw", "MemberOfParliament.tsv"), "r") as f:
        MoP_df = pd.read_csv(f, sep="\t")

    for MoP_xml_str in MoP_df.get("XmlDataFi", []):

        MoP_root = etree.parse(StringIO(MoP_xml_str)).getroot()

        person_id = MoP_root.find(".//HenkiloNro").text

        assemblies = MoP_root.findall(".//NykyisetToimielinjasenyydet/Toimielin") + MoP_root.findall(".//AiemmatToimielinjasenyydet/Toimielin")
        
        for assembly in assemblies:
            if assembly.attrib.get("OnkoValiokunta") == "true":
                name = assembly.find(".//Nimi").text

                # Some instances have no name because the committee does not exist anymore, skip
                if not name:
                    continue

                memberships = assembly.findall(".//Jasenyys")
                for membership in memberships:

                    # Some instances are broken and have no role
                    if membership.find(".//Rooli").text == None:
                        continue

                    role = roles[membership.find(".//Rooli").text.lower()]

                    # Some instances are broken and have no start date
                    if membership.find(".//AlkuPvm") is None or membership.find(".//AlkuPvm").text is None:      
                        continue
                    
                    start_date = membership.find(".//AlkuPvm").text

                    match start_date:
                        # Some old instances have no exact date, just a year or year + roman numerals
                        # In these cases we create dates arbitrarily
                        case s if s.endswith("II"):
                            start_date = f"{s[:4]}-07-01"
                        case s if s.endswith("I"):
                            start_date = f"{s[:4]}-01-01"
                        case s if len(s) < 7:
                            start_date = f"{s[:4]}-01-01"
                        case _:
                            start_date = "-".join(reversed(s.split(".")))

                    if membership.find(".//LoppuPvm") is None:
                        continue

                    end_date = membership.find(".//LoppuPvm").text
                    match end_date:
                        case None:
                            end_date = None
                        # Some old instances have no exact date, just a year or year + roman numerals
                        # In these cases we create dates arbitrarily
                        case e if e.endswith("II"):
                            end_date = f"{e[:4]}-12-31"
                        case e if e.endswith("I"):
                            end_date = f"{e[:4]}-06-30"
                        case e if len(e) < 7:
                            end_date = f"{e[:4]}-12-31"
                        case _:
                            end_date = "-".join(list(reversed((end_date).split("."))))
                
                    mcm_records.append({"person_id": person_id,
                                "committee_name": name, 
                                "start_date": start_date, 
                                "end_date": end_date,
                                "role": role})
                    
    pd.DataFrame(mcm_records).to_csv(csv_path, index=False, encoding="utf-8")


def import_data():
    conn = get_connection()
    cursor = conn.cursor()

    with open(csv_path) as f:
        cursor.copy_expert(
            "COPY mp_committee_memberships(person_id, committee_name, start_date, end_date, role) FROM stdin DELIMITERS ',' CSV HEADER QUOTE '\"';",
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
