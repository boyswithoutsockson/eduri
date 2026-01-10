import os
import pandas as pd
from lxml import etree
from io import StringIO
from XML_parsing_help_functions import (
    id_parse,
    date_parse,
    Nimeke_parse,
    Perustelu_parse,
    Ponsi_parse,
    status_parse,
    Allekirjoittaja_parse,
    NS,
)
from db import get_connection

# Paths
interpellations_tsv_path = os.path.join("data", "raw", "vaski", "Interpellation_fi.tsv")
interpellations_csv = os.path.join("data", "preprocessed", "interpellations.csv")
interpellation_signatures_csv = os.path.join(
    "data", "preprocessed", "interpellation_signatures.csv"
)
handling_tsv_path = os.path.join(
    "data", "raw", "vaski", "KasittelytiedotValtiopaivaasia_fi.tsv"
)


def preprocess_data():
    os.makedirs(os.path.dirname(interpellations_csv), exist_ok=True)
    interpellation_df = pd.read_csv(interpellations_tsv_path, sep="\t")
    handling_df = pd.read_csv(handling_tsv_path, sep="\t")

    interpellation_records = []
    sgn_records = []

    conn = get_connection()
    cur = conn.cursor()

    for interpellation_xml_str in interpellation_df.get("XmlData", []):
        interpellation_root = etree.parse(StringIO(interpellation_xml_str)).getroot()

        eid = id_parse(interpellation_root, NS)

        interpellation = interpellation_root.find(".//kys:Kysymys", namespaces=NS)

        if (
            interpellation is None
        ):  # Joskus oikean välikysymyksen lisäksi on tyhjä välikysymys samalla id:llä
            if (
                len(interpellation_df.loc[interpellation_df["Eduskuntatunnus"] == eid])
                > 1
            ):  # Tarkistetaan että samalla id:llä löytyy toinenkin (oikea) välikysymys
                continue
            else:
                raise Exception

        handling_xml_str = (
            handling_df.loc[handling_df["Eduskuntatunnus"] == eid]
            .get("XmlData", "")
            .tolist()[0]
        )
        handling_root = etree.parse(StringIO(handling_xml_str)).getroot()

        interpellation_records.append(
            {
                "id": eid.lower(),
                "date": date_parse(interpellation_root, NS),
                "title": Nimeke_parse(interpellation, NS),
                "reasoning": Perustelu_parse(interpellation, NS),
                "motion": Ponsi_parse(interpellation, NS),
                "status": status_parse(handling_root, handling_xml_str, NS),
            }
        )

        sgn_records.extend(Allekirjoittaja_parse(interpellation, NS, eid, cur))

    conn.commit()
    cur.close()
    conn.close()

    pd.DataFrame(interpellation_records).to_csv(
        interpellations_csv, index=False, encoding="utf-8"
    )
    pd.DataFrame(sgn_records).drop_duplicates().to_csv(
        interpellation_signatures_csv, index=False, encoding="utf-8"
    )


def import_data():
    conn = get_connection()
    cur = conn.cursor()

    with open(interpellations_csv, "r", encoding="utf-8") as f:
        cur.copy_expert(
            """
            COPY interpellations(id, date, title, reasoning, motion, status)
            FROM STDIN WITH (FORMAT CSV, HEADER TRUE, QUOTE '\"');
            """,
            f,
        )

    with open(interpellation_signatures_csv, "r", encoding="utf-8") as f:
        cur.copy_expert(
            """
            COPY interpellation_signatures(interpellation_id, person_id, first)
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
