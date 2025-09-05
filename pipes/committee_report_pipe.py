import os
import csv
import pandas as pd
import psycopg2
from lxml import etree
from io import StringIO

# Paths
tsv_path = os.path.join("data", "raw", "vaski", "CommitteeReport_fi.tsv")
committee_reports_csv = os.path.join("data", "preprocessed", "committee_reports.csv")
committee_report_signatures_csv = os.path.join("data", "preprocessed", "committee_report_signatures.csv")
objections_csv = os.path.join("data", "preprocessed", "objections.csv")
objection_signatures_csv = os.path.join("data", "preprocessed", "objection_signatures.csv")

# Namespaces
NS = {
    "asi": "http://www.vn.fi/skeemat/asiakirjakooste/2010/04/27",
    "asi1": "http://www.vn.fi/skeemat/asiakirjaelementit/2010/04/27",
    "met": "http://www.vn.fi/skeemat/metatietokooste/2010/04/27",
    "met1": "http://www.vn.fi/skeemat/metatietoelementit/2010/04/27",
    "org": "http://www.vn.fi/skeemat/organisaatiokooste/2010/02/15",
    "org1": "http://www.vn.fi/skeemat/organisaatioelementit/2010/02/15",
    "sis": "http://www.vn.fi/skeemat/sisaltokooste/2010/04/27",
    "sis1": "http://www.vn.fi/skeemat/sisaltoelementit/2010/04/27",
    "vml": "http://www.eduskunta.fi/skeemat/mietinto/2011/01/04",
    "vsk": "http://www.eduskunta.fi/skeemat/vaskikooste/2011/01/04",
    "vsk1": "http://www.eduskunta.fi/skeemat/vaskielementit/2011/01/04",
    "saa": "http://www.vn.fi/skeemat/saadoskooste/2010/04/27",
    "saa1": "http://www.vn.fi/skeemat/saadoselementit/2010/04/27",
    "vas": "http://www.eduskunta.fi/skeemat/vastalause/2011/01/04",  # objections namespace
}

def _txt(node):
    """Collapse all text from an element; return '' if node is None."""
    if node is None:
        return ""
    return " ".join("".join(node.itertext()).split())

def _all_txt(root, xpath):
    """List of collapsed texts for all matches."""
    return [_txt(n) for n in root.findall(xpath, namespaces=NS)]

def preprocess_data():
    os.makedirs(os.path.dirname(committee_reports_csv), exist_ok=True)
    df_tsv = pd.read_csv(tsv_path, sep="\t")

    cr_records = []            # committee_reports rows
    cr_sig_records = []        # committee_report_signatures rows
    objection_records = []     # objections rows
    objection_sig_records = [] # objection_signatures rows (includes local objection_index)

    for xml_str in df_tsv.get("XmlData", []):
        if not isinstance(xml_str, str) or not xml_str.strip():
            continue

        try:
            root = etree.parse(StringIO(xml_str)).getroot()
        except Exception:
            # skip broken XML rows
            continue

        mietinto = root.find(".//vml:Mietinto", namespaces=NS)
        if mietinto is None:
            # Not a standard Mietinto
            continue

        # --- committee_report id (eid) ---
        eid = mietinto.get(f"{{{NS['met1']}}}eduskuntaTunnus", "").strip()
        if not eid:
            typ = _txt(mietinto.find(".//asi:IdentifiointiOsa/asi:EduskuntaTunniste/met1:AsiakirjatyyppiKoodi", namespaces=NS))
            num = _txt(mietinto.find(".//asi:IdentifiointiOsa/asi:EduskuntaTunniste/asi1:AsiakirjaNroTeksti", namespaces=NS))
            year = _txt(mietinto.find(".//asi:IdentifiointiOsa/asi:EduskuntaTunniste/asi1:ValtiopaivavuosiTeksti", namespaces=NS))
            if typ and num and year:
                yr = year if "vp" in year else f"{year} vp"
                eid = f"{typ} {num}/{yr}"

        # --- proposal_id ---
        proposal_id = _txt(mietinto.find(".//asi:IdentifiointiOsa/asi:Vireilletulo/met1:EduskuntaTunnus", namespaces=NS))

        # --- committee_name ---
        node = mietinto.find(".//asi:IdentifiointiOsa/met:Toimija[@met1:rooliKoodi='Laatija']/met1:YhteisoTeksti", namespaces=NS)
        if node is None:
            node = mietinto.find(".//asi:IdentifiointiOsa/met:Toimija/met1:YhteisoTeksti", namespaces=NS)
        committee_name = _txt(node)

        # --- proposal_summary (JOHDANTO + SisältöKuvaus kappaleet) ---
        ps_parts = []
        ps_parts += _all_txt(mietinto, ".//vsk:AsiaKuvaus//sis:KappaleKooste")
        ps_parts += _all_txt(mietinto, ".//asi:SisaltoKuvaus//sis:KappaleKooste")
        proposal_summary = "\n\n".join(p for p in ps_parts if p)

        # --- opinion (vsk:PaatosOsa) ---
        op_parts = []
        op_parts += _all_txt(mietinto, ".//vsk:PaatosOsa//sis1:OtsikkoTeksti")
        op_parts += _all_txt(mietinto, ".//vsk:PaatosOsa//asi1:JohdantoTeksti")
        op_parts += _all_txt(mietinto, ".//vsk:PaatosOsa//sis:KappaleKooste")
        op_parts += _all_txt(mietinto, ".//vsk:PaatosOsa//sis:SisennettyKappaleKooste")
        opinion = "\n\n".join(p for p in op_parts if p)

        # --- report-level reasoning (asi:PerusteluOsa, any subtype) ---
        doc_reason_parts = []
        for po in mietinto.findall(".//asi:PerusteluOsa", namespaces=NS):
            doc_reason_parts += _all_txt(po, ".//sis1:OtsikkoTeksti")
            doc_reason_parts += _all_txt(po, ".//sis1:ValiotsikkoTeksti")
            doc_reason_parts += _all_txt(po, ".//sis:KappaleKooste")
            doc_reason_parts += _all_txt(po, ".//sis:SisennettyKappaleKooste")
        doc_reasoning = "\n\n".join(p for p in doc_reason_parts if p)

        # --- law changes (saa:SaadosOsa -> Markdown) ---
        def saados_to_md(saados):
            title_bits = []
            stype = _txt(saados.find(".//saa:SaadostyyppiKooste", namespaces=NS))
            sname = _txt(saados.find(".//saa:SaadosNimekeKooste", namespaces=NS))
            num = _txt(saados.find(".//saa:LakiehdotusNumeroKooste", namespaces=NS))
            if num: title_bits.append(num.strip(".") + ".")
            if stype: title_bits.append(stype)
            if sname: title_bits.append(sname)
            header = " ".join(title_bits).strip()
            out = []
            if header:
                out.append(f"### {header}")

            # Johtolause (preamble)
            for jl in saados.findall(".//saa:Johtolause", namespaces=NS):
                for p in jl.findall(".//saa:SaadosKappaleKooste", namespaces=NS):
                    txt = _txt(p)
                    if txt:
                        out.append(txt)

            # Pykälät
            for pyk in saados.findall(".//saa:Pykala", namespaces=NS):
                pykno = _txt(pyk.find(".//saa:PykalaTunnusKooste", namespaces=NS))
                ots = _txt(pyk.find(".//saa:SaadosOtsikkoKooste", namespaces=NS))
                head = f"**{pykno} {ots}**".strip()
                if head and head != "** **":
                    out.append(head)

                for mom in pyk.findall(".//saa:MomenttiKooste", namespaces=NS):
                    mtxt = _txt(mom)
                    if mtxt:
                        out.append(mtxt)

                for km in pyk.findall(".//saa:KohdatMomentti", namespaces=NS):
                    johd = _txt(km.find(".//saa:MomenttiJohdantoKooste", namespaces=NS))
                    if johd:
                        out.append(johd)
                    for k in km.findall(".//saa:MomenttiKohtaKooste", namespaces=NS):
                        kt = _txt(k)
                        if kt:
                            out.append(f"- {kt}")

            return "\n\n".join(out)

        law_md_blocks = []
        for saados in mietinto.findall(".//saa:SaadosOsa/saa:Saados", namespaces=NS):
            law_md = saados_to_md(saados)
            if law_md:
                law_md_blocks.append(law_md)
        law_changes = "\n\n---\n\n".join(law_md_blocks)

        # --- committee_report_signatures (vsk:OsallistujaOsa)
        for h in mietinto.findall(".//vsk:OsallistujaOsa//org:Henkilo", namespaces=NS):
            mp_id = h.get(f"{{{NS['met1']}}}muuTunnus", "").strip()
            if mp_id.isdigit():
                cr_sig_records.append({"committee_report_id": eid, "mp_id": int(mp_id)})

        # --- objections (vas:JasenMielipideOsa) + objection signatures
        obj_idx = 0
        for jm in mietinto.findall(".//vas:JasenMielipideOsa", namespaces=NS):
            obj_idx += 1  # 1-based index per report

            # Reasoning = asi:PerusteluOsa -> headers + paragraphs (both sis: and sis1:)
            reason_nodes = []
            reason_nodes += jm.findall(".//asi:PerusteluOsa//sis1:OtsikkoTeksti", namespaces=NS)
            reason_nodes += jm.findall(".//asi:PerusteluOsa//sis1:ValiotsikkoTeksti", namespaces=NS)
            reason_nodes += jm.findall(".//asi:PerusteluOsa//sis:KappaleKooste", namespaces=NS)
            reason_nodes += jm.findall(".//asi:PerusteluOsa//sis:SisennettyKappaleKooste", namespaces=NS)
            reason_nodes += jm.findall(".//asi:PerusteluOsa//sis1:KappaleKooste", namespaces=NS)
            reason_nodes += jm.findall(".//asi:PerusteluOsa//sis1:SisennettyKappaleKooste", namespaces=NS)
            obj_reasoning = "\n\n".join(" ".join("".join(n.itertext()).split()) for n in reason_nodes if n is not None)

            # Motion = asi:PonsiOsa -> johdanto + paragraphs (both sis: and sis1:)
            motion_nodes = []
            motion_nodes += jm.findall(".//asi:PonsiOsa//sis1:OtsikkoTeksti", namespaces=NS)
            motion_nodes += jm.findall(".//asi:PonsiOsa//asi1:JohdantoTeksti", namespaces=NS)
            motion_nodes += jm.findall(".//asi:PonsiOsa//sis:KappaleKooste", namespaces=NS)
            motion_nodes += jm.findall(".//asi:PonsiOsa//sis:SisennettyKappaleKooste", namespaces=NS)
            motion_nodes += jm.findall(".//asi:PonsiOsa//sis1:KappaleKooste", namespaces=NS)
            motion_nodes += jm.findall(".//asi:PonsiOsa//sis1:SisennettyKappaleKooste", namespaces=NS)
            obj_motion = "\n\n".join(" ".join("".join(n.itertext()).split()) for n in motion_nodes if n is not None)

            objection_records.append({
                "committee_report_id": eid,
                "objection_index": obj_idx,
                "reasoning": obj_reasoning,
                "motion": obj_motion
            })

            # objection signatures under this JasenMielipideOsa
            for h in jm.findall(".//asi:Allekirjoittaja//org:Henkilo", namespaces=NS):
                mp_id = h.get(f"{{{NS['met1']}}}muuTunnus", "").strip()
                if mp_id.isdigit():
                    objection_sig_records.append({
                        "committee_report_id": eid,
                        "objection_index": obj_idx,
                        "mp_id": int(mp_id)
                    })

        # --- collect committee report row
        cr_records.append({
            "id": eid,
            "proposal_id": proposal_id,
            "committee_name": committee_name,
            "proposal_summary": proposal_summary,
            "opinion": opinion,
            "reasoning": doc_reasoning,  # report-level reasoning (not objection reasoning)
            "law_changes": law_changes,
        })

    # --- write CSVs
    pd.DataFrame(cr_records).to_csv(committee_reports_csv, index=False, encoding="utf-8")

    df_cr_sigs = pd.DataFrame(cr_sig_records).drop_duplicates()
    if not df_cr_sigs.empty:
        df_cr_sigs["mp_id"] = pd.to_numeric(df_cr_sigs["mp_id"], errors="coerce")
        df_cr_sigs = df_cr_sigs.dropna(subset=["mp_id"])
        df_cr_sigs["mp_id"] = df_cr_sigs["mp_id"].astype(int)
    df_cr_sigs.to_csv(committee_report_signatures_csv, index=False, encoding="utf-8")

    df_objs = pd.DataFrame(objection_records, columns=["committee_report_id", "objection_index", "reasoning", "motion"])
    df_objs.to_csv(objections_csv, index=False, encoding="utf-8")

    df_obj_sigs = pd.DataFrame(objection_sig_records, columns=["committee_report_id", "objection_index", "mp_id"]).drop_duplicates()
    if not df_obj_sigs.empty:
        df_obj_sigs["mp_id"] = pd.to_numeric(df_obj_sigs["mp_id"], errors="coerce")
        df_obj_sigs = df_obj_sigs.dropna(subset=["mp_id"])
        df_obj_sigs["mp_id"] = df_obj_sigs["mp_id"].astype(int)
    df_obj_sigs.to_csv(objection_signatures_csv, index=False, encoding="utf-8")

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
            COPY committee_report_signatures(committee_report_id, mp_id)
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
            cr_id = row["committee_report_id"]
            ob_idx = int(row["objection_index"])
            mp_id = int(row["mp_id"])
            key = (cr_id, ob_idx, mp_id)
            if key in seen:
                continue
            seen.add(key)

            ob_id = obj_id_map.get((cr_id, ob_idx))
            if ob_id is None:
                continue  # no matching objection inserted (safety guard)

            cur.execute(
                """
                INSERT INTO objection_signatures (objection_id, mp_id)
                VALUES (%s, %s);
                """,
                (ob_id, mp_id)
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
