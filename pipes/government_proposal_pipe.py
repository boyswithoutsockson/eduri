import os
import pandas as pd
import psycopg2
from lxml import etree
from io import StringIO

# Paths
tsv_path = os.path.join("data", "raw", "vaski", "GovernmentProposal_fi.tsv")
csv_path = os.path.join("data", "preprocessed", "government_proposals.csv")

# Namespaces
NS = {
    "asi": "http://www.vn.fi/skeemat/asiakirjakooste/2010/04/27",
    "asi1": "http://www.vn.fi/skeemat/asiakirjaelementit/2010/04/27",
    "met": "http://www.vn.fi/skeemat/metatietokooste/2010/04/27",
    "met1": "http://www.vn.fi/skeemat/metatietoelementit/2010/04/27",
    "sis": "http://www.vn.fi/skeemat/sisaltokooste/2010/04/27",
    "sis1": "http://www.vn.fi/skeemat/sisaltoelementit/2010/04/27",
    "saa": "http://www.vn.fi/skeemat/saadoskooste/2010/04/27",
    "saa1": "http://www.vn.fi/skeemat/saadoselementit/2010/04/27",
    "he": "http://www.vn.fi/skeemat/he/2010/04/27",
}

def _txt(node):
    return " ".join("".join(node.itertext()).split()) if node is not None else ""

def _all_txt(root, xpath):
    return [_txt(n) for n in root.findall(xpath, namespaces=NS)]

def preprocess_data():
    os.makedirs(os.path.dirname(csv_path), exist_ok=True)
    df_tsv = pd.read_csv(tsv_path, sep="\t")

    records = []
    for xml_str in df_tsv.get("XmlData", []):
        if not isinstance(xml_str, str) or not xml_str.strip():
            continue

        try:
            root = etree.parse(StringIO(xml_str)).getroot()
        except Exception:
            continue

        esitys = root.find(".//he:HallituksenEsitys", namespaces=NS)
        if esitys is None:
            continue

        # --- id (EduskuntaTunnus) ---
        eid = _txt(esitys.find(".//asi:IdentifiointiOsa/asi:Vireilletulo/met1:EduskuntaTunnus", namespaces=NS))
        if not eid:
            eid = esitys.get(f"{{{NS['met1']}}}eduskuntaTunnus", "").strip()
        if not eid:
            typ = _txt(esitys.find(".//asi:IdentifiointiOsa/asi:EduskuntaTunniste/met1:AsiakirjatyyppiKoodi", namespaces=NS))
            num = _txt(esitys.find(".//asi:IdentifiointiOsa/asi:EduskuntaTunniste/asi1:AsiakirjaNroTeksti", namespaces=NS))
            year = _txt(esitys.find(".//asi:IdentifiointiOsa/asi:EduskuntaTunniste/asi1:ValtiopaivavuosiTeksti", namespaces=NS))
            if typ and num and year:
                yr = year if "vp" in year else f"{year} vp"
                eid = f"{typ} {num}/{yr}"

        # --- title ---
        title = _txt(esitys.find(".//asi:IdentifiointiOsa/met:Nimeke/met1:NimekeTeksti", namespaces=NS))

        # --- proposal_text (SisaltoKuvaus only) ---
        ps_parts = _all_txt(esitys, ".//asi:SisaltoKuvaus//sis:KappaleKooste")
        proposal_text = "\n\n".join(p for p in ps_parts if p)

        # --- reasoning (all PerusteluOsa sections) ---
        reason_parts = []
        for po in esitys.findall(".//asi:PerusteluOsa", namespaces=NS):
            reason_parts += _all_txt(po, ".//sis1:OtsikkoTeksti")
            reason_parts += _all_txt(po, ".//sis1:ValiotsikkoTeksti")
            reason_parts += _all_txt(po, ".//sis:KappaleKooste")
            reason_parts += _all_txt(po, ".//sis:SisennettyKappaleKooste")
        reasoning = "\n\n".join(p for p in reason_parts if p)

        # --- law_changes (SaadosOsa -> Markdown) ---
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
            for jl in saados.findall(".//saa:Johtolause", namespaces=NS):
                for p in jl.findall(".//saa:SaadosKappaleKooste", namespaces=NS):
                    txt = _txt(p)
                    if txt:
                        out.append(txt)
            for pyk in saados.findall(".//saa:Pykala", namespaces=NS):
                pykno = _txt(pyk.find(".//saa:PykalaTunnusKooste", namespaces=NS))
                ots = _txt(pyk.find(".//saa:SaadosOtsikkoKooste", namespaces=NS))
                if pykno or ots:
                    out.append(f"**{pykno} {ots}**".strip())
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

        law_blocks = []
        for saados in esitys.findall(".//saa:SaadosOsa/saa:Saados", namespaces=NS):
            block = saados_to_md(saados)
            if block:
                law_blocks.append(block)
        law_changes = "\n\n---\n\n".join(law_blocks)

        records.append({
            "id": eid,
            "title": title,
            "proposal_text": proposal_text,
            "reasoning": reasoning,
            "law_changes": law_changes,
        })

    pd.DataFrame(records, columns=["id", "title", "proposal_text", "reasoning", "law_changes"]).to_csv(
        csv_path, index=False, encoding="utf-8"
    )

def import_data():
    conn = psycopg2.connect(
        database="postgres", host="db", user="postgres", password="postgres", port="5432"
    )
    cur = conn.cursor()
    with open(csv_path, "r", encoding="utf-8") as f:
        cur.copy_expert(
            """
            COPY government_proposals(id, title, proposal_text, reasoning, law_changes)
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
    parser.add_argument("--preprocess-data", action="store_true")
    parser.add_argument("--import-data", action="store_true")
    args = parser.parse_args()

    if args.preprocess_data:
        preprocess_data()
    if args.import_data:
        import_data()
    if not args.preprocess_data and not args.import_data:
        preprocess_data()
        import_data()
