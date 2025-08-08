import os.path
import csv
import psycopg2
import argparse
import xmltodict
import sys
import json
import pandas as pd


csv.field_size_limit(sys.maxsize)
csv_path = 'data/preprocessed/board_proposal.csv'


def _get_doctype(xml):
    try:
        document_type = xml['ns11:Siirto']['Sanomavalitys']['ns4:SanomatyyppiNimi']['#text']
    except KeyError:
        try:
            document_type = xml['ns11:Siirto']['Sanomavalitys']['ns2:SanomatyyppiNimi']['#text']
        except KeyError:
            document_type = xml['ns11:Siirto']['ns:Sanomavalitys']['ns4:SanomatyyppiNimi']['#text']
    return document_type


def preprocess_data():
    with open(os.path.join("data", "raw", "VaskiData.tsv"), "r") as f:
       vaski = pd.read_csv(f, delimiter="\t", quotechar='"')

    board_proposals = vaski[vaski["Eduskuntatunnus"].str.startswith("HE")]
    board_proposals["XmlData"] = board_proposals["XmlData"].apply(
        lambda x: xmltodict.parse(x))
    board_proposals["doctype"] = board_proposals[board_proposals["XmlData"].apply(
        lambda x: _get_doctype(x)
    )]
    board_proposals = board_proposals[board_proposals["doctype"].str.endswith("GovernmentProposal_fi")]
    print(f"Processing {len(board_proposals)} proposals:")
    bps = []

    for ek_tunnus, bp in list(zip(board_proposals['Eduskuntatunnus'], board_proposals["XmlData"])):
        try:
            bp_brief = xml['ns11:Siirto']['SiirtoAsiakirja']['RakenneAsiakirja']['he:HallituksenEsitys']['asi:SisaltoKuvaus']
            bp_args = xml['ns11:Siirto']['SiirtoAsiakirja']['RakenneAsiakirja']['he:HallituksenEsitys']['asi:PerusteluOsa']
        except KeyError:  # Ehkä talousarvio
            bp_brief = xml['ns11:Siirto']['ns11:SiirtoMetatieto']['jme:JulkaisuMetatieto']['asi:IdentifiointiOsa']['met:Nimeke']['met1:NimekeTeksti']
            bp_args = ""
        if type(bp_brief) != str:
            try:
                bp_brief = "\n\n".join(bp_brief['sis:KappaleKooste'])
            except:
                import pdb;pdb.set_trace()
        if type(bp_args) == list:
            args = ""
            for arg in bp_args:
                args += parse_law_text_to_markdown(arg['asi:PerusteluLuku'])
            bp_args = args
        elif type(bp_args) == str:
            pass
        else:
            bp_args = parse_law_text_to_markdown(bp_args['asi:PerusteluLuku'])
        bps.append({"parliament_id": ek_tunnus, "brief": bp_brief, "reasoning": bp_args})

    bps = pd.DataFrame.from_dict(bps)

    with open(csv_path, 'w') as f:
        bps.to_csv(f, index=False, header=False)
        
def import_data():
    conn = psycopg2.connect(database="postgres",
                            host="db",
                            user="postgres",
                            password="postgres",
                            port="5432")
    cursor = conn.cursor()

    with open(csv_path) as f:
        cursor.copy_expert("COPY board_proposals(parliament_id, brief, reasoning) FROM stdin DELIMITERS ',' CSV QUOTE '\"';", f)
        
    conn.commit()
    cursor.close()
    conn.close()

def parse_law_text_to_markdown(data):
    def process_kappale_kooste(kooste):
        if isinstance(kooste, str):
            return kooste.strip()
        elif isinstance(kooste, list):
            paragraphs = []
            for item in kooste:
                if isinstance(item, dict):
                    kursiivi = item.get('sis1:KursiiviTeksti', '')
                    text = item.get('#text', '')
                    if kursiivi:
                        paragraphs.append(f"*{kursiivi}* {text}".strip())
                    else:
                        paragraphs.append(text.strip())
                elif isinstance(item, str):
                    paragraphs.append(item.strip())
            return "\n\n".join(paragraphs)
        return ""

    def process_perusteluluku(perustelut):
        parts = []
        if isinstance(perustelut, dict):
            perustelut = [perustelut]
        for perustelu in perustelut:
            otsikko = perustelu.get('sis:LukuOtsikko', {})
            nro = otsikko.get('sis1:OtsikkoNroTeksti', '').strip()
            teksti = otsikko.get('sis1:OtsikkoTeksti', '').strip()
            if nro or teksti:
                parts.append(f"## {nro} {teksti}".strip())

            kappaleet = perustelu.get('sis:KappaleKooste', '')
            if kappaleet:
                parts.append(process_kappale_kooste(kappaleet))
        return "\n\n".join(parts)

    markdown_parts = []
    output = ""

    if type(data) == dict:
        data = [data]

    for section in data:
        if type(section) == dict:
            if 'asi:PerusteluLuku' in section.keys():
                output += parse_law_text_to_markdown(section['asi:PerusteluLuku'])
        try:
            otsikko = section.get('sis:LukuOtsikko', {})
        except:
            import pdb;pdb.set_trace()
        nro = otsikko.get('sis1:OtsikkoNroTeksti', '').strip()
        teksti = otsikko.get('sis1:OtsikkoTeksti', '').strip()
        if nro or teksti:
            markdown_parts.append(f"# {nro} {teksti}".strip())

        perustelut = section.get('asi:PerusteluLuku', [])
        kappaleet = section.get('sis:KappaleKooste', '')

        if perustelut:
            markdown_parts.append(process_perusteluluku(perustelut))
        elif kappaleet:
            markdown_parts.append(process_kappale_kooste(kappaleet))

    output += "\n\n".join(markdown_parts).strip()

    return output


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--preprocess-data", help="preprocess the data", action="store_true")
    parser.add_argument("--import-data", help="import preprocessed data", action="store_true")
    args = parser.parse_args()
    if args.preprocess_data:
        preprocess_data()
    if args.import_data:
        import_data()
    if not args.preprocess_data and not args.import_data:
        preprocess_data()
        import_data()
