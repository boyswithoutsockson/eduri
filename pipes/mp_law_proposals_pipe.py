import os
import pandas as pd
from lxml import etree
from io import StringIO
from XML_parsing_help_functions import (
    id_parse,
    date_parse,
    Nimeke_parse,
    AsiaSisaltoKuvaus_parse,
    Perustelu_parse,
    Saados_parse,
    status_parse,
    Allekirjoittaja_parse,
    NS,
)
from db import get_connection

# Paths
mp_proposal_tsv_path = os.path.join("data", "raw", "vaski", "LegislativeMotion_fi.tsv")
mp_proposals_csv = os.path.join("data", "preprocessed", "mp_law_proposals.csv")
mp_proposal_signatures_csv = os.path.join(
    "data", "preprocessed", "mp_law_proposal_signatures.csv"
)
handling_tsv_path = os.path.join(
    "data", "raw", "vaski", "KasittelytiedotValtiopaivaasia_fi.tsv"
)


def preprocess_data():
    os.makedirs(os.path.dirname(mp_proposals_csv), exist_ok=True)
    mpp_df = pd.read_csv(mp_proposal_tsv_path, sep="\t")
    handling_df = pd.read_csv(handling_tsv_path, sep="\t")

    mpp_records = []
    sgn_records = []

    conn = get_connection()
    cur = conn.cursor()

    for mpp_xml_str in mpp_df.get("XmlData", []):
        mpp_root = etree.parse(StringIO(mpp_xml_str)).getroot()

        eid = id_parse(mpp_root, NS)

        date = date_parse(mpp_root, NS)

        proposal = mpp_root.find(".//eka:Lakialoite", namespaces=NS)
        if (
            proposal is None
        ):  # Joskus oikean aloitteen lisäksi on tyhjä aloite samalla id:llä
            if (
                len(mpp_df.loc[mpp_df["Eduskuntatunnus"] == eid]) > 1
            ):  # Tarkistetaan että samalla id:llä löytyy toinenkin (oikea) aloite
                continue  # Skipataan tämä
            elif (
                eid == "LA 78/2017 vp"
            ):  # 2017 itsenäisyyspäivänä perustettu Itsenäisyyden juhlavuoden lastensäätiö perustettiin lakialoitteena
                continue
            else:
                raise Exception

        handling_xml_str = (
            handling_df.loc[handling_df["Eduskuntatunnus"] == eid]
            .get("XmlData", "")
            .tolist()[0]
        )
        handling_root = etree.parse(StringIO(handling_xml_str)).getroot()

        mpp_records.append(
            {
                "id": eid.lower(),
                "ptype": "mp_law",
                "date": date,
                "title": Nimeke_parse(proposal, NS),
                "summary": AsiaSisaltoKuvaus_parse(proposal, NS),
                "reasoning": Perustelu_parse(proposal, NS),
                "law_changes": Saados_parse(proposal, NS),
                "status": status_parse(handling_root, handling_xml_str, NS),
            }
        )

        sgn_records.extend(Allekirjoittaja_parse(proposal, NS, eid, cur))

    conn.commit()
    cur.close()
    conn.close()

    pd.DataFrame(mpp_records).to_csv(mp_proposals_csv, index=False, encoding="utf-8")
    pd.DataFrame(sgn_records).drop_duplicates().to_csv(
        mp_proposal_signatures_csv, index=False, encoding="utf-8"
    )


def import_data():
    conn = get_connection()
    cur = conn.cursor()

    with open(mp_proposals_csv, "r", encoding="utf-8") as f:
        cur.copy_expert(
            """
            COPY proposals(id, ptype, date, title, summary, reasoning, law_changes, status)
            FROM STDIN WITH (FORMAT CSV, HEADER TRUE, QUOTE '\"');
            """,
            f,
        )

    with open(mp_proposal_signatures_csv, "r", encoding="utf-8") as f:
        cur.copy_expert(
            """
            COPY proposal_signatures(proposal_id, person_id, first)
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
