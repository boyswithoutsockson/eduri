import os
import pandas as pd
import psycopg2
from lxml import etree
from io import StringIO

def _txt(node):
    """Collapse all text from an element; return '' if node is None."""
    if node is None:
        return ""
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

def parse_tau_table(table_element):
    """
    Parse a <tau:table> XML element into a JSON-friendly dict:
    {
      "title": "...",
      "rows": [
        {"column_1": "...", "column_2": "...", "column_3": ["...", "..."]},
        ...
      ]
    }
    """
    namespaces = {'tau': 'tau', 'sis': 'sis'}

    result = {}
    # Capture table title if it exists
    title_elem = table_element.find(".//tau:title/sis:KappaleKooste", namespaces)
    if title_elem is not None:
        result["title"] = title_elem.text.strip()

    rows = []
    for row in table_element.findall(".//tau:row", namespaces):
        entries = row.findall("tau:entry", namespaces)
        row_data = {}
        for idx, entry in enumerate(entries, start=1):
            # Collect all kappale texts from the entry
            kappale_list = entry.findall("sis:KappaleKooste", namespaces)
            values = [k.text.strip() for k in kappale_list if k.text and k.text.strip()]

            if not values and entry.text and entry.text.strip():
                values.append(entry.text.strip())

            # Store list if multiple values, string if single, None if empty
            row_data[f"column_{idx}"] = values[0] if len(values) == 1 else (values or None)
        rows.append(row_data)

    result["rows"] = rows
    return result

def Perustelu_parse(root, NS, not_child_of=""):
    if not_child_of:
        filter = f"[not(ancestor::{not_child_of})]"
    else:
        filter = ""

    reasoning_nodes = root.xpath(
            f".//asi:PerusteluOsa{filter}//sis1:OtsikkoTeksti | "
            f".//asi:PerusteluOsa{filter}//sis1:ValiotsikkoTeksti | "
            f".//asi:PerusteluOsa{filter}//sis:KappaleKooste | "
            f".//asi:PerusteluOsa{filter}//sis:SisennettyKappaleKooste",
            namespaces=NS
        )
    reasoning = "\n\n".join([_txt(n) for n in reasoning_nodes])

    return reasoning

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
            f".//vsk:AsiaKuvaus//sis:KappaleKooste{filter} | "
            f".//asi:SisaltoKuvaus//sis:KappaleKooste{filter}",
            namespaces=NS
        )
    summary = "\n\n".join([_txt(n) for n in summary_nodes])

    return summary

def Ponsi_parse(root, NS):
    ponsi_nodes = root.xpath(
            f".//asi:PonsiOsa//sis1:OtsikkoTeksti | "
            f".//asi:PonsiOsa//asi1:JohdantoTeksti | "
            f".//asi:PonsiOsa//sis:KappaleKooste | "
            f".//asi:PonsiOsa//sis:SisennettyKappaleKooste |"
            f".//asi:PonsiOsa//sis1:KappaleKooste |"
            f".//asi:PonsiOsa//sis1:SisennettyKappaleKooste",
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
            f".//vsk:PaatosOsa//sis:KappaleKooste{filter} | "
            f".//vsk:PaatosOsa//sis:SisennettyKappaleKooste{filter}",
            namespaces=NS
        )
    opinion = "\n\n".join([_txt(n) for n in op_nodes])

    return opinion

def Allekirjoittaja_parse(root, NS, eid, cursor=None):
    sgn_records = []
    for signer in root.findall(".//asi:Allekirjoittaja", namespaces=NS):
            if signer.find(".//org:Henkilo/org1:EtuNimi", namespaces=NS).text is None:       # Joskus nää on vaan jostain syystä tyhjiä
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

            if signer.attrib.get(f"{{{NS['asi1']}}}allekirjoitusLuokitusKoodi") == "EnsimmainenAllekirjoittaja":     # Hallitusten esityksissä ei kai käytetä tätä systeemiä
                first = 1                                                                                       # joten tää on käytännössä vielä testaamatta
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