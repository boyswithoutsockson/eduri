import pandas as pd
from lxml import etree
from io import StringIO
from xml.etree import ElementTree
from xml.etree.ElementTree import Element
from xml.dom import minidom


def prettify(elem):
    """Return a pretty-printed XML string for the Element."""
    rough_string = ElementTree.tostring(elem, "utf-8")
    reparsed = minidom.parseString(rough_string)
    return reparsed.toprettyxml(indent="  ")


def pp(elem):
    print(prettify(elem))


class ParserError(Exception):
    """Custom error for handling parser edge cases"""


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
    "vas": "http://www.eduskunta.fi/skeemat/vastalause/2011/01/04",
    "jme": "http://www.eduskunta.fi/skeemat/julkaisusiirtokooste/2011/12/20",
    "ns11": "http://www.eduskunta.fi/skeemat/siirto/2011/09/07",
    "ns4": "http://www.eduskunta.fi/skeemat/siirtoelementit/2011/05/17",
    "s359": "http://www.vn.fi/skeemat/metatietoelementit/2010/04/27",
    "s360": "http://www.vn.fi/skeemat/metatietoelementit/2010/04/27",
    "sii": "http://www.eduskunta.fi/skeemat/siirtokooste/2011/05/17",
    "sii1": "http://www.eduskunta.fi/skeemat/siirtoelementit/2011/05/17",
    "he": "http://www.vn.fi/skeemat/he/2010/04/27",
    "tau": "http://www.vn.fi/skeemat/taulukkokooste/2010/04/27",
    "mix": "http://www.loc.gov/mix/v20",
    "narc": "http://www.narc.fi/sahke2/2010-09_vnk",
    "kys": "http://www.eduskunta.fi/skeemat/kysymys/2012/08/10",
    "xsi": "http://www.w3.org/2001/XMLSchema-instance",
    "def": "http://www.eduskunta.fi/skeemat/siirtokooste/2011/05/17",
    "eka": "http://www.eduskunta.fi/skeemat/eduskuntaaloite/2012/08/10",
    "ptk": "http://www.eduskunta.fi/skeemat/poytakirja/2011/01/28",
    "ns1": "http://www.vn.fi/skeemat/sisaltoelementit/2010/04/27",
    "ns2": "http://www.vn.fi/skeemat/sisaltoelementit/2010/04/27",
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
    if num:
        title_bits.append(num)
    if stype:
        title_bits.append(stype)
    if sname:
        title_bits.append(sname)
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
                    namespaces=NS,
                )
                values = [
                    k.text.strip() for k in kappale_list if k.text and k.text.strip()
                ]
                if not values and entry.text and entry.text.strip():
                    values.append(entry.text.strip())
                if col_n in columns.keys():
                    columns[col_n].append(" <br> ".join(values) if values else "")
                else:
                    columns[col_n] = [(" <br> ".join(values) if values else "")]

        df = pd.DataFrame({k: pd.Series(v) for k, v in columns.items()})
        df.columns = df.iloc[0]  # first row becomes header
        df = df.drop(df.index[0])  # remove that row from data
        dfs.append(df.to_markdown(index=False))

    return "\n\n".join(dfs)


def get_tag_type(element: Element) -> str:
    """
    For an element, returns the tag type without namespace, for example

    `'{http://www.vn.fi/skeemat/sisaltoelementit/2010/04/27}OtsikkoTeksti'`
    =>
    `'OtsikkoTeksti'`
    """
    return element.tag.split("}")[1]


def OtsikkoTeksti_parse(element: Element, level: int):
    """Parses an <OtsikkoTeksti> into a heading of the appropriate level"""
    if level < 1 or level > 6:
        raise ParserError(f"Heading level can't be {level}!")
    if element.text is None:
        return ""  # broken otsikkoteksti node
    return f"{'#' * level} {element.text.capitalize()}\n\n"


def KursiiviTeksti_parse(element: Element):
    """Parses <KursiiviTeksti> inline element to italics"""
    return f"*{element.text.strip()}* "


def LihavaTeksti_parse(element: Element):
    """Parses <LihavaTeksti> inline element to bold"""
    return f"**{element.text.strip()}** "


def LihavaKursiiviTeksti_parse(element: Element):
    """Parses <LihavaKursiiviTeksti> inline element to bold italics"""
    return f"***{element.text.strip()}*** "


def KappaleKooste_parse(element: Element):
    """
    The main XML parsing function, handling all of the different leaf nodes of
    the XML tree.
    """

    def _iter_kappale_content(elem: Element):
        if elem.text:
            yield ("text", elem.text)
        for child in elem:
            yield ("element", child)
            if child.tail:
                yield ("text", child.tail)

    out = ""
    citations = []
    for type, value in _iter_kappale_content(element):
        match type:
            case "text":
                out += value
            case "element":
                match get_tag_type(value):
                    case (
                        "KursiiviTeksti"
                        | "HarvaKursiiviTeksti"
                        | "LihavaTeksti"
                        | "LihavaKursiiviTeksti"
                    ) if value.text is None:
                        continue  # sometimes there just isn't any content
                    case "KursiiviTeksti" | "HarvaKursiiviTeksti":
                        out += KursiiviTeksti_parse(value)
                    case "LihavaTeksti":
                        out += LihavaTeksti_parse(value)
                    case "LihavaKursiiviTeksti":
                        out += LihavaKursiiviTeksti_parse(value)

                    case "YlaindeksiTeksti" if value.text is None:
                        pass  # Some documents have empty XML nodes like this
                    case "YlaindeksiTeksti" if value.text.strip() == "2":
                        out += "²"
                    case "YlaindeksiTeksti" if value.text.strip() == "3":
                        out += "³"
                    case "YlaindeksiTeksti":
                        out += f"<sup>{value.text}</sup>"
                    case "AlaindeksiTeksti" if value.text.strip() == "1":
                        out += "₁"
                    case "AlaindeksiTeksti" if value.text.strip() == "2":
                        out += "₂"
                    case "AlaindeksiTeksti" if value.text.strip() == "3":
                        out += "₃"
                    case "AlaindeksiTeksti" if value.text.strip() == "10":
                        out += "₁₀"  # for example "PM10"
                    case "AlaindeksiTeksti":
                        out += f"<sub>{value.text}</sub>"
                    case "AlaviiteTeksti":
                        out += f"[^{id(value.text)}]"
                        citations.append(f"[^{id(value.text)}]: {value.text}")
                    case "YleinenViite":
                        out += f"[{value[0].text}]({value.get(f'{{{NS["ns1"]}}}viiteURL')})"
                    case "AlaviiteKooste":
                        out += f"({KappaleKooste_parse(value).strip()})"
                    case "Rivivaihto":
                        # TODO: this is not a good solution, the documents look like there
                        # would be a better way to parse these than to force a line break
                        out += "  \n"
                    case "Aukko":
                        # TODO: might be unnecessary? extra whitespace isn't too bad, though
                        out += " "
                    case (
                        "SaadoskokoelmaViiteTunnus"
                        | "AsiakirjaViiteTunnus"
                        | "SopimussarjaViiteTunnus"
                    ):
                        # No special meaning (yet)
                        # TODO: we could use these to add hyperlinks to the related documents
                        out += value.text

                    case unknown:
                        raise ParserError(f"Unknown tag: {unknown}", value.text, value)

    if len(citations) > 0:
        return out + "\n\n" + "\n\n".join(citations) + "\n\n"
    else:
        return out + "\n\n"


def Kuva_parse(element: Element):
    """
    Parses a <Kuva> element. This will assume that the image asset exists
    in the same relative path as whatever document holding it.
    """
    if len(element) == 0:
        return ""  # broken image node
    url = element[0].get(f"{{{NS['ns1']}}}kuvaTiedostoTeksti")
    return f"![]({url})\n\n"


def Lista_parse(element: Element):
    """
    Parses an XML <Lista> element to markdown, normalizing different list
    bullet styles to just ordered or unordered markdown list items.
    """
    match element.get(f"{{{NS['ns2']}}}ulkoasuKoodi"):
        case "Viiva" | "Tasaviiva" | "LyhytTasaviiva":
            return (
                "".join(f"- {KappaleKooste_parse(item[0])}\n" for item in element)
                + "\n\n"
            )
        case "Numerosulku" | "Numeropiste":
            return (
                "".join(
                    f"{idx + 1}. {KappaleKooste_parse(item[0])}\n"
                    for idx, item in enumerate(element)
                )
                + "\n\n"
            )
        case "Tyhja":  # most likely a table of contents that can be skipped
            return ""
        case unknown:
            raise ParserError(f"Unknown list type: {unknown}", element)


def SuppeaLista_parse(element: Element):
    """
    Parses an XML <SuppeaLista> element to markdown.
    This differs from <Lista> in that each list item is wrapped in its own
    <SuppeaLista>, e.g.:

    ```xml
    <Lista>
        <Alkio>dataa 1</Alkio>
        <Alkio>dataa 2</Alkio>
    </Lista>
    ```
    vs
    ```xml
    <SuppeaLista>
        <Alkio>dataa 1</Alkio>
    </SuppeaLista>
    <SuppeaLista>
        <Alkio>dataa 2</Alkio>
    </SuppeaLista>

    The reason for this distinction to ever exist is left as an exercise for the reader.
    ```
    """
    out = ""
    match element.get(f"{{{NS['ns2']}}}ulkoasuKoodi"):
        case (
            "Numeropiste"
            | "JatkuvaNumeropiste"
            | "JatkuvaNumerosulku"
            | "Numerosulku"
            | "Kirjainsulku"
        ):
            out = "".join(
                f"{idx + 1}. {KappaleKooste_parse(child)}\n"
                for idx, child in enumerate(element)
            )
        case "Viiva" | "Tasaviiva" | "LyhytViiva" | "LyhytTasaviiva":
            out = "".join(f"- {KappaleKooste_parse(child)}\n" for child in element)
        case unknown:
            raise ParserError(f"Unknown shallow list type: {unknown}", element)
    next_element = element.getnext()
    next_element_tag = get_tag_type(next_element) if next_element is not None else None
    if next_element_tag != "SuppeaLista":
        out += "\n"
    return out


def xml_to_markdown(element: Element, level: int = 1):
    """
    Generic Vaski XML parser aiming to handle all text formatting cases and nested
    heading levels via recursion.
    """
    match get_tag_type(element):
        # parse actual contents
        case (
            "OtsikkoTeksti"
            | "ValiotsikkoTeksti"
            | "LihavaKursiiviOtsikkoTeksti"
            | "RiviotsikkoTeksti"
        ):
            return OtsikkoTeksti_parse(element, level)
        case "KappaleKooste" | "JohdantoTeksti" | "ViiteTeksti":
            return KappaleKooste_parse(element)
        case "SisennettyKappaleKooste":
            return f"> {KappaleKooste_parse(element)}"
        case "table":
            return tau_to_md(element)
        case "Kuva":
            return Kuva_parse(element)
        case "Lista":
            return Lista_parse(element)
        case "SuppeaLista":
            return SuppeaLista_parse(element)

        # skip these tags and recurse deeper
        case (
            "LukuOtsikko"
            | "PerusteluOsa"
            | "SisaltoKuvaus"
            | "AsiaKuvaus"
            | "PaatosOsa"
            | "PaatosToimenpide"
            | "PonsiOsa"
            | "PykalaViite"
        ):
            return "".join(xml_to_markdown(el, level) for el in element)
        # this denotes a subchapter so we increment level and recurse
        case "PerusteluLuku" | "VireilletuloAsia":
            return "".join(xml_to_markdown(el, level + 1) for el in element)

        # these tags have no contents or we dont care about them, end this tail of recursion
        case (
            "OtsikkoNroTeksti"
            | "Tyhja"
            | "NeljannesTyhja"
            | "AsiantuntijatToimenpide"
            | "Valiokuntakasittely"
            | "MuuAsiaKuvaus"
            | "YhdistettyAsia"
        ):  # committee reports pipe
            return ""

        case unknown_tag:
            raise ParserError(f"Unknown tag: {unknown_tag}", element)


def PerusteluOsa_parse_to_markdown(root: Element, NS):
    """Finds and recursively parses `PerusteluOsa` from a root xml node"""
    reasoning_part = root.find(".//asi:PerusteluOsa", namespaces=NS)
    if reasoning_part is None:
        return None
    return xml_to_markdown(reasoning_part)


def AsiaSisaltoKuvaus_parse_to_markdown(root: Element, NS):
    """Finds all `AsiaKuvaus` and `SisaltoKuvaus` nodes and parses them to markdown"""
    # TODO: are all of these relevant?
    summary_parts = root.xpath(
        ".//vsk:AsiaKuvaus | .//asi:SisaltoKuvaus | .//asi:AsiaKuvaus", namespaces=NS
    )
    return "\n\n".join(xml_to_markdown(part) for part in summary_parts)


def PaatosOsa_parse_to_markdown(root: Element, NS):
    """Finds and recursively parses `PaatosOsa` from a root xml node"""
    opinion_parts = root.xpath(".//vsk:PaatosOsa | .//asi:PaatosOsa", namespaces=NS)
    return "\n\n".join(xml_to_markdown(part) for part in opinion_parts)


def Ponsi_parse_to_markdown(root: Element, NS):
    """Finds and recursively parses `Ponsi` from a root xml node"""
    ponsi_part = root.find(".//asi:PonsiOsa", namespaces=NS)
    return xml_to_markdown(ponsi_part)


def date_parse(root, NS):
    metadata = root.find(".//jme:JulkaisuMetatieto", namespaces=NS)
    date = metadata.get(f"{{{NS['met1']}}}laadintaPvm", "").strip()

    return date


def Nimeke_parse(root, NS):
    title_nodes = root.xpath(".//met:Nimeke//met1:NimekeTeksti", namespaces=NS)
    title = "\n\n".join([_txt(n) for n in title_nodes])

    return title


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
    if (
        handling_root.find(".//vsk:EduskuntakasittelyPaatosKuvaus", namespaces=NS)
        is None
    ):
        return "open"
    status = handling_root.find(
        ".//vsk:EduskuntakasittelyPaatosKuvaus", namespaces=NS
    ).attrib.get(f"{{{NS['vsk1']}}}eduskuntakasittelyPaatosKoodi")
    match status:
        case None:
            status = "open"
        case (
            "Expired"
            | "Cancelled"
            | "Rejected"
            | "Resting"
            | "Passed"
            | "Replied"
            | "Dealt"
        ):
            status = status.lower()
        case "PassedChanged":
            status = "passed_changed"
        case "PassedUrgent":
            status = "passed_urgent"
        case _:
            Exception

    return status


def rollcall_id_parse(root):
    rollcall_id_node = root.xpath(
        ".//vsk:MuuAsiakohta//vsk:KohtaAsiakirja[met1:AsiakirjatyyppiNimi[contains(., 'Nimenhuutoraportti')]]/@vsk1:hyperlinkkiKoodi",
        namespaces=NS,
    )

    if rollcall_id_node:
        return rollcall_id_node[0]
    else:
        return


def absentee_parse(root):
    absentees = []

    for absentee in root.xpath(".//met:Toimija/org:Henkilo", namespaces=NS):
        person_id = int(absentee.get(f"{{{NS['met1']}}}muuTunnus"))

        # Collect all lisatieto texts
        lisatiedot = absentee.xpath("org1:LisatietoTeksti/text()", namespaces=NS)

        # Flag True if "(e)" is among them (e = work related reason for absence)
        work_related = "(e)" in lisatiedot

        absentees.append({"person_id": person_id, "work_related": work_related})

    return absentees


def Allekirjoittaja_parse(root, NS, eid, cursor=None):
    sgn_records = []
    for signer in root.findall(".//asi:Allekirjoittaja", namespaces=NS):
        if (
            signer.find(".//org:Henkilo/org1:EtuNimi", namespaces=NS) is None
        ):  # Joskus nää on vaan jostain syystä tyhjiä
            continue

        person_id = signer.find(".//org:Henkilo", namespaces=NS).attrib.get(
            f"{{{NS['met1']}}}muuTunnus"
        )
        if person_id == "*":
            continue

        if person_id is None:
            first_name = signer.find(".//org:Henkilo/org1:EtuNimi", namespaces=NS).text
            last_name = signer.find(".//org:Henkilo/org1:SukuNimi", namespaces=NS).text
            if not first_name or not last_name:  # Joskus nääki voi puuttua huoh
                continue

            # Joskus sukunimen yhteydessä on puolue
            if len(last_name.split()) > 1 and last_name.split()[-1].endswith(
                ("ps", "kok", "vihr", "sd", "r", "liik", "kesk", "vas")
            ):
                last_name = "".join(last_name.split()[:-1]).strip()

            cursor.execute(
                """
                SELECT id 
                FROM persons
                WHERE LOWER(first_name) = ? AND LOWER(last_name) = ?""",
                (first_name.strip().lower(), last_name.strip().lower()),
            )

            person_id = cursor.fetchone()
            if person_id is not None:
                person_id = person_id[0]
            else:
                # Tänne menee sihteerit yms. jotka on joskus allekirjoittamassa esityksiä
                continue

        if (
            signer.attrib.get(f"{{{NS['asi1']}}}allekirjoitusLuokitusKoodi")
            == "EnsimmainenAllekirjoittaja"
        ):  # Hallitusten esityksissä ei kai käytetä tätä systeemiä
            first = 1  # joten tää on käytännössä vielä testaamatta
        else:
            first = 0

        sgn_records.append(
            {
                "government_proposal_id": eid.lower(),
                "person_id": int(person_id),
                "first": first,
            }
        )
    return sgn_records


def Osallistuja_parse(root, NS, eid):
    sgn_records = []
    for person in root.findall(".//vsk:OsallistujaOsa//org:Henkilo", namespaces=NS):
        person_id = person.get(f"{{{NS['met1']}}}muuTunnus", "").strip()
        if person_id.isdigit():
            sgn_records.append(
                {"committee_report_id": eid.lower(), "person_id": int(person_id)}
            )

    return sgn_records


def id_parse(root, NS):
    metadata = root.find(".//jme:JulkaisuMetatieto", namespaces=NS)
    eid = metadata.get(f"{{{NS['met1']}}}eduskuntaTunnus", "").strip()

    return eid
