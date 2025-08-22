import os
import pandas as pd
import psycopg2
from lxml import etree
from io import StringIO

# Paths
tsv_path = os.path.join("data", "raw", "vaski", "CommitteeReport_fi.tsv")
csv_path = os.path.join("data", "preprocessed", "committee_reports.csv")
signatures_path = os.path.join("data", "preprocessed", "signatures.csv")

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
    os.makedirs(os.path.dirname(csv_path), exist_ok=True)
    df_tsv = pd.read_csv(tsv_path, sep="\t")

    records = []
    sig_records = []

    for i, xml_str in enumerate(df_tsv.get("XmlData", []), start=1):
        if not isinstance(xml_str, str) or not xml_str.strip():
            continue

        try:
            root = etree.parse(StringIO(xml_str)).getroot()
        except Exception:
            # Broken XML row — skip
            continue

        mietinto = root.find(".//vml:Mietinto", namespaces=NS)
        if mietinto is None:
            # Not a standard Mietinto (e.g., TalousarvioMietinto) — skip row
            continue

        # --- id ---
        eid = mietinto.get(f"{{{NS['met1']}}}eduskuntaTunnus", "").strip()
        if not eid:
            typ = _txt(mietinto.find(".//asi:IdentifiointiOsa/asi:EduskuntaTunniste/met1:AsiakirjatyyppiKoodi", namespaces=NS))
            num = _txt(mietinto.find(".//asi:IdentifiointiOsa/asi:EduskuntaTunniste/asi1:AsiakirjaNroTeksti", namespaces=NS))
            year = _txt(mietinto.find(".//asi:IdentifiointiOsa/asi:EduskuntaTunniste/asi1:ValtiopaivavuosiTeksti", namespaces=NS))
            if typ and num and year:
                yr = year if "vp" in year else f"{year} vp"
                eid = f"{typ} {num}/{yr}"

        # --- reference_id ---
        reference_id = _txt(mietinto.find(".//asi:IdentifiointiOsa/asi:Vireilletulo/met1:EduskuntaTunnus", namespaces=NS))

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

        # --- reasoning (asi:PerusteluOsa, any subtype) ---
        reason_parts = []
        for po in mietinto.findall(".//asi:PerusteluOsa", namespaces=NS):
            reason_parts += _all_txt(po, ".//sis1:OtsikkoTeksti")
            reason_parts += _all_txt(po, ".//sis1:ValiotsikkoTeksti")
            reason_parts += _all_txt(po, ".//sis:KappaleKooste")
            reason_parts += _all_txt(po, ".//sis:SisennettyKappaleKooste")
        reasoning = "\n\n".join(p for p in reason_parts if p)

        # --- law_changes (saa:SaadosOsa -> Markdown) ---
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
                if head == "** **":
                    head = ""
                if head:
                    out.append(head)

                # Moments (paragraphs)
                for mom in pyk.findall(".//saa:MomenttiKooste", namespaces=NS):
                    mtxt = _txt(mom)
                    if mtxt:
                        out.append(mtxt)

                # “KohdatMomentti” (bulleted sub-points)
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

        # --- signatures (vsk:OsallistujaOsa -> org:Henkilo@met1:muuTunnus) ---
        for h in mietinto.findall(".//vsk:OsallistujaOsa//org:Henkilo", namespaces=NS):
            mp_id = h.get(f"{{{NS['met1']}}}muuTunnus", "").strip()
            if mp_id:
                sig_records.append({"committee_report_id": eid, "mp_id": mp_id})

        records.append({
            "id": eid,
            "reference_id": reference_id,
            "committee_name": committee_name,
            "proposal_summary": proposal_summary,
            "opinion": opinion,
            "reasoning": reasoning,
            "law_changes": law_changes,
        })

    # Write CSVs
    df_out = pd.DataFrame(records, columns=[
        "id",
        "reference_id",
        "committee_name",
        "proposal_summary",
        "opinion",
        "reasoning",
        "law_changes",
    ])

    df_out.to_csv(csv_path, index=False, encoding="utf-8")


    sig_records_df = pd.DataFrame(sig_records, columns=["committee_report_id", "mp_id"])

    # Vaski-datassa on virhe: Saman mp_idn taakse on kirjattu useita eri nimiä
    # Tämä johtaa tilanteeseen, jossa kansanedustaja voi 'allekirjoittaa'
    # lausunnon useamman kerran, mikä kaataa ohjelman kantaan kirjoituksen aikana,
    # sillä duplikaattiallekirjoituksia ei ole sallittu tauluun. 
    # Virhe on kohtalaisen pieni, 16 mietinnön kohdalla joitain kymmeniä henkilöitä on
    # kirjattu väärällä mp_idllä. Virheen mittakaavan huomioiden jätetään tässä kohtaa
    # virheellinen data korjaamatta, vaikka nimitietoja hyödyntäen se olisi teoriassa 
    # mahdollista. Sen sijaan poistetaan duplikaatit ja säilytetään vain ensimmäinen löytö.
    sig_records_df = sig_records_df[~sig_records_df.duplicated(keep='first')]
    
    sig_records_df.to_csv(
        signatures_path, index=False, encoding="utf-8"
    )

def import_data():
    conn = psycopg2.connect(
        database="postgres",
        host="db",
        user="postgres",
        password="postgres",
        port="5432"
    )
    cur = conn.cursor()
    # committee_reports
    with open(csv_path, "r", encoding="utf-8") as f:
        cur.copy_expert(
            """
            COPY committee_reports(id, reference_id, committee_name, proposal_summary, opinion, reasoning, law_changes)
            FROM STDIN WITH (FORMAT CSV, HEADER TRUE, QUOTE '\"');
            """,
            f
        )
    # signatures
    with open(signatures_path, "r", encoding="utf-8") as f:
        cur.copy_expert(
            """
            COPY signatures(committee_report_id, mp_id)
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
