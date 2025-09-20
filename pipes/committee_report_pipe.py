import os
import csv
import pandas as pd
import psycopg2
from lxml import etree
from io import StringIO
from XML_parsing_help_functions import _txt, id_parse, AsiaSisaltoKuvaus_parse, Perustelu_parse, Ponsi_parse, Saados_parse, Paatos_parse, Osallistuja_parse

# Paths
tsv_path = os.path.join("data", "raw", "vaski", "CommitteeReport_fi.tsv")
committee_reports_csv = os.path.join("data", "preprocessed", "committee_reports.csv")
committee_report_signatures_csv = os.path.join("data", "preprocessed", "committee_report_signatures.csv")
objections_csv = os.path.join("data", "preprocessed", "objections.csv")
objection_signatures_csv = os.path.join("data", "preprocessed", "objection_signatures.csv")

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

def _txt(node):
    """Collapse all text from an element; return '' if node is None."""
    if node is None:
        return ""
    return " ".join("".join(node.itertext()).split())

def preprocess_data():

    conn = psycopg2.connect(
        database="postgres",
        host="db",
        user="postgres",
        password="postgres",
        port="5432"
    )
    cur = conn.cursor()

    os.makedirs(os.path.dirname(committee_reports_csv), exist_ok=True)
    df_tsv = pd.read_csv(tsv_path, sep="\t")

    cr_records = []            # committee_reports rows
    cr_sgn_records = []        # committee_report_signatures rows
    objection_records = []     # objections rows
    objection_sgn_records = [] # objection_signatures rows (includes local objection_index)

    for xml_str in df_tsv.get("XmlData", []):

        root = etree.parse(StringIO(xml_str)).getroot()

        mietinto = root.find(".//vml:Mietinto", namespaces=NS)
        if mietinto is None:
            # Talousarviomietinnöt (TalousarvioMietinto) skipataan vielä tässä vaiheessa, koska ne on niin erilaisia
            # NE PITÄÄ IMPLEMENTOIDA
            continue

        # --- committee_report id (eid) ---
        eid = id_parse(root, NS)

        # --- proposal_id ---
        proposal_id = _txt(mietinto.find(".//asi:IdentifiointiOsa/asi:Vireilletulo/met1:EduskuntaTunnus", namespaces=NS)).lower()

        # --- committee_name ---
        node = mietinto.find(".//asi:IdentifiointiOsa/met:Toimija[@met1:rooliKoodi='Laatija']/met1:YhteisoTeksti", namespaces=NS)
        if node is None:
            node = mietinto.find(".//asi:IdentifiointiOsa/met:Toimija/met1:YhteisoTeksti", namespaces=NS)
        committee_name = _txt(node)

        # --- proposal_summary (restrict to content NOT under objections) ---
        # Using XPath to exclude any descendants that live inside vas:JasenMielipideOsa
        proposal_summary = AsiaSisaltoKuvaus_parse(mietinto, NS, not_child_of="vas:JasenMielipideOsa")

        # --- opinion (vsk:PaatosOsa), excluding any objection subtrees ---
        opinion = Paatos_parse(root, NS, not_child_of="vas:JasenMielipideOsa")

        # --- report-level reasoning (exclude objection reasoning) ---
        reasoning = Perustelu_parse(mietinto, NS, not_child_of="vas:JasenMielipideOsa")

        # --- law changes (saa:SaadosOsa -> Markdown) ---
        law_changes = Saados_parse(root, NS)

        # --- committee_report_signatures (vsk:OsallistujaOsa)
        cr_sgn_records.extend(Osallistuja_parse(root, NS, eid))

        # --- objections (vas:JasenMielipideOsa) + objection signatures
        obj_idx = 0
        for objection in mietinto.findall(".//vas:JasenMielipideOsa", namespaces=NS):
            obj_idx += 1  # 1-based index per report

            # Reasoning = asi:PerusteluOsa -> headers + paragraphs (both sis: and sis1:)
            obj_reasoning = Perustelu_parse(objection, NS)

            # Motion = asi:PonsiOsa -> johdanto + paragraphs (both sis: and sis1:)
            obj_motion = Ponsi_parse(objection, NS)

            objection_records.append({
                "committee_report_id": eid.lower(),
                "objection_index": obj_idx,
                "reasoning": obj_reasoning,
                "motion": obj_motion
            })

            # objection signatures under this JasenMielipideOsa
            for signer in objection.findall(".//asi:Allekirjoittaja", namespaces=NS):
                if signer is None:
                    continue
                person_id = signer.find(".//org:Henkilo", namespaces=NS).attrib.get(f"{{{NS['met1']}}}muuTunnus")
                if person_id is None:
                    first_name = signer.find(".//org:Henkilo/org1:EtuNimi", namespaces=NS).text
                    last_name = signer.find(".//org:Henkilo/org1:SukuNimi", namespaces=NS).text
                    if not first_name or not last_name:                         # Joskus nääki voi puuttua huoh
                        continue
                    # Joskus sukunimen yhteydessä on puolue
                    if len(last_name.split()) > 1 and last_name.split()[-1].endswith(("ps", "kok", "vihr", "sd", "r", "liik", "kesk", "vas")):
                        last_name = "".join(last_name.split()[:-1]).strip()
                    cur.execute("""
                        SELECT id 
                        FROM public.persons 
                        WHERE LOWER(first_name) = %s AND LOWER(last_name) = %s""", 
                        (first_name.strip().lower(), last_name.strip().lower())
                        )
                    
                    person_id = cur.fetchone()
                    if person_id is not None:
                        person_id = person_id[0]
                    else:
                        # Tänne menee sihteerit yms. jotka on joskus allekirjoittamassa esityksiä
                        continue
                objection_sgn_records.append({
                    "committee_report_id": eid,
                    "objection_index": obj_idx,
                    "person_id": int(person_id)
                })

        

        # --- collect committee report row (check for duplicates)
        cr_records.append({
            "id": eid.lower(),
            "proposal_id": proposal_id,
            "committee_name": committee_name,
            "proposal_summary": proposal_summary,
            "opinion": opinion,
            "reasoning": reasoning,  # report-level reasoning (not objection reasoning)
            "law_changes": law_changes,
        })

    conn.commit()
    cur.close()
    conn.close()
    

    # Write CSVs
    pd.DataFrame(cr_records).to_csv(committee_reports_csv, index=False, encoding="utf-8")
    
    # Vaski-datassa on virhe: Saman persp_idn taakse on kirjattu useita eri nimiä
    # Tämä johtaa tilanteeseen, jossa kansanedustaja voi 'allekirjoittaa'
    # lausunnon useamman kerran, mikä kaataa ohjelman kantaan kirjoituksen aikana,
    # sillä duplikaattiallekirjoituksia ei ole sallittu tauluun. 
    # Virhe on kohtalaisen pieni, 16 mietinnön kohdalla joitain kymmeniä henkilöitä on
    # kirjattu väärällä person_idllä. Virheen mittakaavan huomioiden jätetään tässä kohtaa
    # virheellinen data korjaamatta, vaikka nimitietoja hyödyntäen se olisi teoriassa 
    # mahdollista. Sen sijaan poistetaan duplikaatit ja säilytetään vain ensimmäinen löytö.
    df_cr_sgns = pd.DataFrame(cr_sgn_records).drop_duplicates(subset=["committee_report_id", "person_id"])
    if not df_cr_sgns.empty:
        df_cr_sgns["person_id"] = pd.to_numeric(df_cr_sgns["person_id"], errors="coerce")
        df_cr_sgns = df_cr_sgns.dropna(subset=["person_id"])
        df_cr_sgns["person_id"] = df_cr_sgns["person_id"].astype(int)
    df_cr_sgns.to_csv(committee_report_signatures_csv, index=False, encoding="utf-8")

    df_objs = pd.DataFrame(objection_records, columns=["committee_report_id", "objection_index", "reasoning", "motion"])
    df_objs.to_csv(objections_csv, index=False, encoding="utf-8")

    df_obj_sgns = pd.DataFrame(objection_sgn_records, columns=["committee_report_id", "objection_index", "person_id"]).drop_duplicates(subset=["committee_report_id", "objection_index", "person_id"])
    if not df_obj_sgns.empty:
        df_obj_sgns["person_id"] = pd.to_numeric(df_obj_sgns["person_id"], errors="coerce")
        df_obj_sgns = df_obj_sgns.dropna(subset=["person_id"])
        df_obj_sgns["person_id"] = df_obj_sgns["person_id"].astype(int)
    df_obj_sgns.to_csv(objection_signatures_csv, index=False, encoding="utf-8")
    

def import_data():
    conn = psycopg2.connect(
        database="postgres",
        host="db",
        user="postgres",
        password="postgres",
        port="5432"
    )
    cur = conn.cursor()

    # 1) committee_reports
    with open(committee_reports_csv, "r", encoding="utf-8") as f:
        cur.copy_expert(
            """
            COPY committee_reports(id, proposal_id, committee_name, proposal_summary, opinion, reasoning, law_changes)
            FROM STDIN WITH (FORMAT CSV, HEADER TRUE, QUOTE '\"');
            """,
            f
        )

    # 2) committee_report_signatures
    with open(committee_report_signatures_csv, "r", encoding="utf-8") as f:
        cur.copy_expert(
            """
            COPY committee_report_signatures(committee_report_id, person_id)
            FROM STDIN WITH (FORMAT CSV, HEADER TRUE, QUOTE '\"');
            """,
            f
        )

    # 3) objections — insert row-by-row to get SERIAL id and map via (committee_report_id, objection_index)
    obj_id_map = {}  # (committee_report_id, objection_index) -> objection_id

    with open(objections_csv, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            cr_id = row["committee_report_id"]
            ob_idx = int(row["objection_index"])
            reasoning = row.get("reasoning", "")
            motion = row.get("motion", "")
            cur.execute(
                """
                INSERT INTO objections (committee_report_id, reasoning, motion)
                VALUES (%s, %s, %s)
                RETURNING id;
                """,
                (cr_id, reasoning, motion)
            )
            new_id = cur.fetchone()[0]
            obj_id_map[(cr_id, ob_idx)] = new_id

    # 4) objection_signatures — map each to its correct objection via the local index
    with open(objection_signatures_csv, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        seen = set()
        for row in reader:
            cr_id = row["committee_report_id"].lower()
            ob_idx = int(row["objection_index"])
            person_id = int(row["person_id"])
            key = (cr_id, ob_idx, person_id)
            if key in seen:
                continue
            seen.add(key)
            ob_id = obj_id_map.get((cr_id, ob_idx))
            if ob_id is None:
                continue  # safety guard
            cur.execute(
                """
                INSERT INTO objection_signatures (objection_id, person_id)
                VALUES (%s, %s);
                """,
                (ob_id, person_id)
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
