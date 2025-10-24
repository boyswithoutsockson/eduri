import os
import pandas as pd
import psycopg2
from lxml import etree
from io import StringIO

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
    elif node.tag == "{http://www.vn.fi/skeemat/taulukkokooste/2010/04/27}table":
        return tau_to_md(node)
    return " ".join("".join(node.itertext()).split())

def saados_to_md(saados, NS):
    title_bits = []
    stype = _txt(saados.find(".//saa:SaadostyyppiKooste", namespaces=NS))
    sname = _txt(saados.find(".//saa:SaadosNimekeKooste", namespaces=NS))
    num = _txt(saados.find(".//saa:LakiehdotusNumeroKooste", namespaces=NS))
    if num: title_bits.append(num)
    if stype: title_bits.append(stype)
    if sname: title_bits.append(sname)
    header = " ".join(title_bits).strip()
    out = []
    if header:
        out.append(f"# {header}\n")

    # Johtolause (preamble)
    for jl in saados.findall(".//saa:Johtolause", namespaces=NS):
        for p in jl.findall(".//saa:SaadosKappaleKooste", namespaces=NS):
            table_md = ""
            for table in saados.findall(".//tau:table", NS):
                table_md = tau_to_md(table)
            txt = _txt(p) + table_md
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
                for table in saados.findall(".//tau:table", namespaces=NS):
                    table_md = tau_to_md(table)
                    out.append(table_md)

            for k in km.findall(".//saa:MomenttiKohtaKooste", namespaces=NS):
                kt = _txt(k)
                if kt:
                    out.append(f"- {kt}")
                    for table in saados.findall(".//tau:table", namespaces=NS):
                        table_md = tau_to_md(table)
                        out.append(table_md)

    return "\n\n".join(out)

def tau_to_md(root):
    tables = []
    dfs = []
    for tgroup in root.findall(".//tau:tgroup", NS):
        # Skip if it looks like a title block (only 1 column)
        colspecs = tgroup.findall("tau:colspec", NS)
        if len(colspecs) <= 1:
            continue
        
        columns = {}
        for row in tgroup.findall(".//tau:row", NS):
            entries = row.findall("tau:entry", NS)
            for entry in entries:
                col_n = entry.get("colname", "")
                kappale_list = entry.xpath(
                    ".//sis:KappaleKooste | "
                    ".//saa:SaadosKappaleKooste | "
                    ".//sis1:LihavaTeksti",
                    namespaces=NS
                )
                values = [k.text.strip() for k in kappale_list if k.text and k.text.strip()]
                if not values and entry.text and entry.text.strip():
                    values.append(entry.text.strip())
                if col_n in columns.keys():
                    columns[col_n].append(" <br> ".join(values) if values else "")
                else:
                    columns[col_n] = [(" <br> ".join(values) if values else "")]

        df = pd.DataFrame({k: pd.Series(v) for k, v in columns.items()})
        df.columns = df.iloc[0]         # first row becomes header
        df = df.drop(df.index[0])       # remove that row from data
        dfs.append(df.to_markdown(index=False))
        
    return "\n\n".join(dfs)


def Perustelu_parse(root, NS, not_child_of=""):
    if not_child_of:
        filter = f"[not(ancestor::{not_child_of})]"
    else:
        filter = ""

    reasoning_nodes = root.xpath(
            f".//asi:PerusteluOsa{filter}//sis1:OtsikkoTeksti | "
            f".//asi:PerusteluOsa{filter}//sis1:ValiotsikkoTeksti | "
            f".//asi:PerusteluOsa{filter}//sis:KappaleKooste[not(ancestor::tau:table)] | "
            f".//asi:PerusteluOsa{filter}//tau:table | "
            f".//asi:PerusteluOsa{filter}//sis:SisennettyKappaleKooste[not(ancestor::tau:table)]",
            namespaces=NS
        )
    reasoning = "\n\n".join([_txt(n) for n in reasoning_nodes])

    return reasoning

def date_parse(root, NS):

    metadata = root.find(".//jme:JulkaisuMetatieto", namespaces=NS)
    date = metadata.get(f"{{{NS['met1']}}}laadintaPvm", "").strip()

    return date

def Nimeke_parse(root, NS):
    title_nodes = root.xpath(
            f".//met:Nimeke//met1:NimekeTeksti",
            namespaces=NS
        )
    title = "\n\n".join([_txt(n) for n in title_nodes])

    return title

def AsiaSisaltoKuvaus_parse(root, NS, not_child_of=""):
    if not_child_of:
        filter = f"[not(ancestor::{not_child_of})]"
    else:
        filter = ""

    summary_nodes = root.xpath(
            f".//vsk:AsiaKuvaus//sis:KappaleKooste[not(ancestor::tau:table)]{filter} | "
            f".//asi:SisaltoKuvaus//sis:KappaleKooste[not(ancestor::tau:table)]{filter} | "
            f".//asi:AsiaKuvaus//tau:table | "
            f".//asi:SisaltoKuvaus//tau:table",
            namespaces=NS
        )
    summary = "\n\n".join([_txt(n) for n in summary_nodes])

    return summary

def Ponsi_parse(root, NS):
    ponsi_nodes = root.xpath(
            f".//asi:PonsiOsa//sis1:OtsikkoTeksti | "
            f".//asi:PonsiOsa//asi1:JohdantoTeksti | "
            f".//asi:PonsiOsa//sis:KappaleKooste[not(ancestor::tau:table)] | "
            f".//asi:PonsiOsa//sis:SisennettyKappaleKooste[not(ancestor::tau:table)] |"
            f".//asi:PonsiOsa//sis1:KappaleKooste[not(ancestor::tau:table)] |"
            f".//asi:PonsiOsa//sis1:SisennettyKappaleKooste[not(ancestor::tau:table)] |"
            f".//asi:PonsiOsa//tau:table",
            namespaces=NS
        )
    ponsi = "\n\n".join([_txt(n) for n in ponsi_nodes])

    return ponsi


def Saados_parse(root, NS):
    law_md_blocks = []
    for saados in root.findall(".//saa:SaadosOsa/saa:Saados", namespaces=NS):
        law_md = saados_to_md(saados, NS)
        if law_md:
            law_md_blocks.append(law_md)

    law_changes = "\n\n---\n\n".join(law_md_blocks)

    return law_changes

def status_parse(handling_root, handling_xml_str, NS):
    handling_root = etree.parse(StringIO(handling_xml_str)).getroot()
    if handling_root.find(".//vsk:EduskuntakasittelyPaatosKuvaus", namespaces=NS) is None:
        return "open"
    status = handling_root.find(".//vsk:EduskuntakasittelyPaatosKuvaus", namespaces=NS).attrib.get(f"{{{NS['vsk1']}}}eduskuntakasittelyPaatosKoodi")
    match status:
        case None:
            status = "open"
        case "Expired" | "Cancelled" | "Rejected" | "Resting" | "Passed":
            status = status.lower()
        case "PassedChanged":
            status = "passed_changed"
        case "PassedUrgent":
            status = "passed_urgent"
        case _:
            Exception

    return status

def Paatos_parse(root, NS, not_child_of=""):
    if not_child_of:
        filter = f"[not(ancestor::{not_child_of})]"
    else:
        filter = ""

    op_nodes = root.xpath(
            f".//vsk:PaatosOsa//sis1:OtsikkoTeksti{filter} | "
            f".//vsk:PaatosOsa//asi1:JohdantoTeksti{filter} | "
            f".//vsk:PaatosOsa//sis:KappaleKooste[not(ancestor::tau:table)]{filter} | "
            f".//vsk:PaatosOsa//sis:SisennettyKappaleKooste[not(ancestor::tau:table)]{filter} |"
            f".//asi:PaatosOsa//tau:table",
            namespaces=NS
        )
    opinion = "\n\n".join([_txt(n) for n in op_nodes])

    return opinion

def Allekirjoittaja_parse(root, NS, eid, cursor=None):
    sgn_records = []
    for signer in root.findall(".//asi:Allekirjoittaja", namespaces=NS):
        if signer.find(".//org:Henkilo/org1:EtuNimi", namespaces=NS) is None:       # Joskus nää on vaan jostain syystä tyhjiä
            continue
        
        person_id = signer.find(".//org:Henkilo", namespaces=NS).attrib.get(f"{{{NS['met1']}}}muuTunnus")
        if person_id == '*':
            continue

        if person_id is None:
            first_name = signer.find(".//org:Henkilo/org1:EtuNimi", namespaces=NS).text
            last_name = signer.find(".//org:Henkilo/org1:SukuNimi", namespaces=NS).text
            if not first_name or not last_name:                         # Joskus nääki voi puuttua huoh
                continue

            # Joskus sukunimen yhteydessä on puolue
            if len(last_name.split()) > 1 and last_name.split()[-1].endswith(("ps", "kok", "vihr", "sd", "r", "liik", "kesk", "vas")):
                last_name = "".join(last_name.split()[:-1]).strip()

            cursor.execute("""
                SELECT id 
                FROM persons
                WHERE LOWER(first_name) = %s AND LOWER(last_name) = %s""", 
                (first_name.strip().lower(), last_name.strip().lower())
                )
            
            person_id = cursor.fetchone()
            if person_id is not None:
                person_id = person_id[0]
            else:
                # Tänne menee sihteerit yms. jotka on joskus allekirjoittamassa esityksiä
                continue

        if signer.attrib.get(f"{{{NS['asi1']}}}allekirjoitusLuokitusKoodi") == "EnsimmainenAllekirjoittaja":  # Hallitusten esityksissä ei kai käytetä tätä systeemiä
            first = 1                                                                                         # joten tää on käytännössä vielä testaamatta
        else:
            first = 0

        sgn_records.append({
            "government_proposal_id": eid.lower(),
            "person_id": int(person_id),
            "first": first
        })
    return sgn_records

def Osallistuja_parse(root, NS, eid):
    sgn_records = []
    for person in root.findall(".//vsk:OsallistujaOsa//org:Henkilo", namespaces=NS):
            person_id = person.get(f"{{{NS['met1']}}}muuTunnus", "").strip()
            if person_id.isdigit():
                sgn_records.append({"committee_report_id": eid.lower(), "person_id": int(person_id)})

    return sgn_records

def id_parse(root, NS):
    metadata = root.find(".//jme:JulkaisuMetatieto", namespaces=NS)
    eid = metadata.get(f"{{{NS['met1']}}}eduskuntaTunnus", "").strip()

    return eid
