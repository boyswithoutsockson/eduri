import os
import csv
import pandas as pd
import psycopg2
from lxml import etree
from io import StringIO
from XML_parsing_help_functions import id_parse, date_parse, Nimeke_parse, AsiaSisaltoKuvaus_parse, parse_reasoning_chapters, Saados_parse, Allekirjoittaja_parse
from db import get_connection

# Paths
mp_petition_tsv_path = os.path.join("data", "raw", "vaski", "PetitionaryMotion_fi.tsv")
mp_petitions_csv = os.path.join("data", "preprocessed", "mp_petition_proposals.csv")
mp_petition_proposal_reasonings_csv = os.path.join("data", "preprocessed", "mp_petition_proposal_reasonings.csv")
mp_petition_signatures_csv = os.path.join("data", "preprocessed", "mp_petition_proposal_signatures.csv")
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
    'def': 'http://www.eduskunta.fi/skeemat/siirtokooste/2011/05/17',
    'eka': "http://www.eduskunta.fi/skeemat/eduskuntaaloite/2012/08/10"
}

def preprocess_data():
    os.makedirs(os.path.dirname(mp_petitions_csv), exist_ok=True)
    mpp_df = pd.read_csv(mp_petition_tsv_path, sep="\t")
    handling_df = pd.read_csv(handling_tsv_path, sep="\t")

    mpp_records = []           
    sgn_records = [] 

    conn = get_connection()
    cur = conn.cursor()   

    cur.execute("""
                SELECT parliament_id 
                FROM agenda_items""")

    agenda_items = cur.fetchall()
    handled_petitions = [petition[0] for petition in agenda_items if petition[0].startswith("tpa")]
    reasonings = []

    for mpp_xml_str in mpp_df.get("XmlData", []):

        mpp_root = etree.parse(StringIO(mpp_xml_str)).getroot()

        eid = id_parse(mpp_root, NS)

        date = date_parse(mpp_root, NS)

        if eid.lower() in handled_petitions:
            status = "handled"
        else:
            status = "open"

        proposal = mpp_root.find(".//eka:EduskuntaAloite", namespaces=NS)
        if proposal is None:                                                # Joskus oikean aloitteen lisäksi on tyhjä aloite samalla id:llä                         
            if len(mpp_df.loc[mpp_df['Eduskuntatunnus'] == eid]) > 1:       # Tarkistetaan että samalla id:llä löytyy toinenkin (oikea) aloite
                continue                                                    # Skipataan tämä           
            else:
                raise Exception

        mpp_records.append({
            "id": eid.lower(),
            "ptype": "mp_petition",
            "date": date,
            "title": Nimeke_parse(proposal, NS),
            "summary": AsiaSisaltoKuvaus_parse(proposal, NS),
            "law_changes": Saados_parse(proposal, NS),
            "status": status
            })
        
        sgn_records.extend(Allekirjoittaja_parse(proposal, NS, eid, cur))
        reasonings += parse_reasoning_chapters(proposal, NS, eid.lower())


    conn.commit()
    cur.close()
    conn.close()  

    pd.DataFrame(mpp_records).to_csv(mp_petitions_csv, index=False, encoding="utf-8")
    pd.DataFrame(sgn_records).drop_duplicates().to_csv(mp_petition_signatures_csv, index=False, encoding="utf-8") 
    pd.DataFrame(reasonings).to_csv(mp_petition_proposal_reasonings_csv, index=False, encoding="utf-8") 


def import_data():
    conn = get_connection()
    cur = conn.cursor()

    with open(mp_petitions_csv, "r", encoding="utf-8") as f:
        cur.copy_expert(
            """
            COPY proposals(id, ptype, date, title, summary, law_changes, status)
            FROM STDIN WITH (FORMAT CSV, HEADER TRUE, QUOTE '\"');
            """,
            f
        )

    with open(mp_petition_proposal_reasonings_csv, "r", encoding="utf-8") as f:
        cur.copy_expert(
            """
            COPY proposal_reasoning(proposal_id, title, position, content)
            FROM STDIN WITH (FORMAT CSV, HEADER TRUE, QUOTE '\"');
            """,
            f
        )

    with open(mp_petition_signatures_csv, "r", encoding="utf-8") as f:
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