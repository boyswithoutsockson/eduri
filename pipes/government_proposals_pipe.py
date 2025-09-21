import os
import csv
import pandas as pd
import psycopg2
from lxml import etree
from io import StringIO
from XML_parsing_help_functions import id_parse, date_parse, Nimeke_parse, AsiaSisaltoKuvaus_parse, Perustelu_parse, Saados_parse, status_parse, Allekirjoittaja_parse

# Paths
gp_tsv_path = os.path.join("data", "raw", "vaski", "GovernmentProposal_fi.tsv")
government_proposals_csv = os.path.join("data", "preprocessed", "government_proposals.csv")
government_proposal_signatures_csv = os.path.join("data", "preprocessed", "government_proposal_signatures.csv")
handling_tsv_path = os.path.join("data", "raw", "vaski", "KasittelytiedotValtiopaivaasia_fi.tsv")

# Namespaces
NS = {
    'asi': 'http://www.vn.fi/skeemat/asiakirjakooste/2010/04/27',
    'asi1': 'http://www.vn.fi/skeemat/asiakirjaelementit/2010/04/27',
    'met': 'http://www.vn.fi/skeemat/metatietokooste/2010/04/27',
    'met1': 'http://www.vn.fi/skeemat/metatietoelementit/2010/04/27',
    'org': 'http://www.vn.fi/skeemat/organisaatiokooste/2010/02/15',
    'org1': 'http://www.vn.fi/skeemat/organisaatioelementit/2010/02/15',
    'sis': 'http://www.vn.fi/skeemat/sisaltokooste/2010/04/27',
    'sis1': 'http://www.vn.fi/skeemat/sisaltoelementit/2010/04/27',
    'vml': 'http://www.eduskunta.fi/skeemat/mietinto/2011/01/04',
    'vsk': 'http://www.eduskunta.fi/skeemat/vaskikooste/2011/01/04',
    'vsk1': 'http://www.eduskunta.fi/skeemat/vaskielementit/2011/01/04',
    'saa': 'http://www.vn.fi/skeemat/saadoskooste/2010/04/27',
    'saa1': 'http://www.vn.fi/skeemat/saadoselementit/2010/04/27',
    'vas': 'http://www.eduskunta.fi/skeemat/vastalause/2011/01/04',
    'jme': 'http://www.eduskunta.fi/skeemat/julkaisusiirtokooste/2011/12/20',
    'ns11': 'http://www.eduskunta.fi/skeemat/siirto/2011/09/07',
    'ns4': 'http://www.eduskunta.fi/skeemat/siirtoelementit/2011/05/17',
    's359': 'http://www.vn.fi/skeemat/metatietoelementit/2010/04/27',
    's360': 'http://www.vn.fi/skeemat/metatietoelementit/2010/04/27',
    'sii': 'http://www.eduskunta.fi/skeemat/siirtokooste/2011/05/17',
    'sii1': 'http://www.eduskunta.fi/skeemat/siirtoelementit/2011/05/17',
    'he': 'http://www.vn.fi/skeemat/he/2010/04/27',
    'tau': 'http://www.vn.fi/skeemat/taulukkokooste/2010/04/27',
    'mix': 'http://www.loc.gov/mix/v20',
    'narc': 'http://www.narc.fi/sahke2/2010-09_vnk',
    'xsi': 'http://www.w3.org/2001/XMLSchema-instance',
    'def': 'http://www.eduskunta.fi/skeemat/siirtokooste/2011/05/17'
}


def preprocess_data():
    os.makedirs(os.path.dirname(government_proposals_csv), exist_ok=True)
    gp_df = pd.read_csv(gp_tsv_path, sep="\t")
    handling_df = pd.read_csv(handling_tsv_path, sep="\t")

    gp_records = []            # government_proposals rows
    sgn_records = []  

    conn = psycopg2.connect(
        database="postgres",
        host="db",
        user="postgres",
        password="postgres",
        port="5432"
        )

    cur = conn.cursor()          

    for gp_xml_str in gp_df.get("XmlData", []):

        gp_root = etree.parse(StringIO(gp_xml_str)).getroot()

        # ID
        eid = id_parse(gp_root, NS)
        if eid[:2] == "RP":                                                             # Joskus tänne on sattunu ruotsinkielisiä versioita                
            if gp_df.loc[gp_df['Eduskuntatunnus'] == f"HE{eid[2:-2]}vp"] is not None:   # Tsekataan löytyykö sama suomeksi
                continue                                                                # Jos löytyy niin skipataan ruotsinkielinen versio ja 
            else:                                                                       # oletetaan että suomenkielinen on tulossa/mennyt
                raise Exception

        date = date_parse(gp_root, NS)

        proposal = gp_root.find(".//he:HallituksenEsitys", namespaces=NS)
        if proposal is None:
            continue

        # TITLE
        title = Nimeke_parse(proposal, NS)
        
        # SUMMARY
        summary = AsiaSisaltoKuvaus_parse(proposal, NS)

        # REASONING
        reasoning = Perustelu_parse(proposal, NS)

        # LAW_CHANGES
        law_changes = Saados_parse(proposal, NS)

        # STATUS
        handling_xml_str = handling_df.loc[handling_df['Eduskuntatunnus'] == eid].get("XmlData", "").tolist()[0] 
        handling_root = etree.parse(StringIO(handling_xml_str)).getroot()
        status = status_parse(handling_root, handling_xml_str, NS)

        gp_records.append({
                "id": eid.lower(),
                "ptype": "government",
                "date": date,
                "title": title,
                "summary": summary,
                "reasoning": reasoning,
                "law_changes": law_changes,
                "status": status
            })

        # SIGNATURES
        sgn_records.extend(Allekirjoittaja_parse(proposal, NS, eid, cur))

    conn.commit()
    cur.close()
    conn.close()

    pd.DataFrame(gp_records).to_csv(government_proposals_csv, index=False, encoding="utf-8")
    pd.DataFrame(sgn_records).to_csv(government_proposal_signatures_csv, index=False, encoding="utf-8")


def import_data():
    conn = psycopg2.connect(
        database="postgres",
        host="db",
        user="postgres",
        password="postgres",
        port="5432"
    )
    cur = conn.cursor()

    with open(government_proposals_csv, "r", encoding="utf-8") as f:
        cur.copy_expert(
            """
            COPY proposals(id, ptype, date, title, summary, reasoning, law_changes, status)
            FROM STDIN WITH (FORMAT CSV, HEADER TRUE, QUOTE '\"');
            """,
            f
        )

    with open(government_proposal_signatures_csv, "r", encoding="utf-8") as f:
        cur.copy_expert(
            """
            COPY proposal_signatures(proposal_id, person_id, first)
            FROM STDIN WITH (FORMAT CSV, HEADER TRUE, QUOTE '\"');
            """,
            f
        )
        
    conn.commit()
    cur.close()
    conn.close()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--preprocess-data", action="store_true", help="Parse TSV and write CSVs")
    parser.add_argument("--import-data", action="store_true", help="Import CSVs into Postgres")
    args = parser.parse_args()

    if args.preprocess_data:
        preprocess_data()
    if args.import_data:
        import_data()
    if not args.preprocess_data and not args.import_data:
        preprocess_data()
        import_data()