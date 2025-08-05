import os.path
import csv
import psycopg2
import argparse
import xmltodict
import sys
from tqdm import tqdm
import pandas as pd


class IncompleteDecisionTreeException(Exception):
    # This exception is used in development to show where the 
    # decision tree is incomplete while parsing xml into string
    pass


csv.field_size_limit(sys.maxsize)
csv_path = 'data/preprocessed/board_proposal.csv'


def preprocess_data():
    with open(os.path.join("data", "raw", "vaski", "GovernmentProposal_fi.tsv"), "r") as f:
       board_proposals = pd.read_csv(f, delimiter="\t", quotechar='"')

    board_proposals["Xml"] = board_proposals["XmlData"].apply(
        lambda x: xmltodict.parse(x))
    
    print(f"Processing {len(board_proposals)} proposals:")
    bps = []

    for ek_tunnus, xml in tqdm(list(zip(board_proposals['Eduskuntatunnus'], board_proposals["Xml"]))):
        bp_brief, bp_full = parse_law_text_to_markdown(xml)
        bps.append({"parliament_id": ek_tunnus, "brief": bp_brief, "reasoning": bp_full})

    bps = pd.DataFrame.from_dict(bps)

    with open(csv_path, 'w') as f:
        bps.to_csv(f, index=False, header=True)
        
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
    def find_relevant_xml(xml):
        # There are varying structures for the xml files in the data
        # In this function we try and locate the relevant segment of the xml file and return it.
        
        def parse_with_identifiointiosa(bp_id, bp_brief, bp_full, level=1):
            def parse_asi_or_sis(xml, level=1):
                def parse_sub_title(xml, level):
                    output = "#" * level
                    if 'sis1:LukuOtsikko' in xml.keys():
                        output += xml['sis1:LukuOtsikko']
                    elif 'sis1:OtsikkoTeksti' in xml.keys():
                        output += xml['sis1:OtsikkoTeksti']
                    else:
                        IncompleteDecisionTreeException
                    output += "\n\n"
                    return output
                
                def parse_formatted_text(section):
                    def parse_str_or_lst(v):
                        # Formatted text bits contain both strings and lists
                        output = ""
                        if isinstance(v, str):
                            output += v
                        elif isinstance(v, list):
                            v = [v_ for v_ in v if v_ is not None]
                            output += " ".join(v)
                        else:
                            raise IncompleteDecisionTreeException
                        return output
                    
                    def parse_links_dict_or_list(v):
                        output = ""
                        if isinstance(v, dict):
                            output += '[' + v['sis1:ViiteTeksti'] + ']'
                            output += '(' + v['@sis1:viiteURL'] + ')'
                        elif isinstance(v, list):
                            for d in v:
                                output += parse_links_dict_or_list(d)
                        return output

                    
                    output = ""
                    if isinstance(section, str):
                        output += section
                    elif isinstance(section, dict):
                        for k, v in section.items():
                            if v is None:
                                # No need for formatting if no value
                                continue
                            

                            if k == '#text':
                                output += parse_str_or_lst(v)
                            elif k == 'sis1:LihavaTeksti':
                                output += '**'
                                output += parse_str_or_lst(v)
                                output += '**'
                            elif k == 'sis1:KursiiviTeksti' or k == 'sis1:HarvaKursiiviTeksti':
                                output += '__'
                                output += parse_str_or_lst(v)
                                output += '__'
                            elif k == 'sis1:LihavaKursiiviTeksti':
                                output += '***'
                                output += parse_str_or_lst(v)
                                output += '***'
                            elif k == 'sis1:YlaindeksiTeksti':
                                output += '<sup>'  # Markdown uses html tags for sup and sub script
                                output += parse_str_or_lst(v)
                                output += '</sup>'
                            elif k == 'sis1:AlaviiteTeksti' or k == 'sis1:AlaindeksiTeksti':
                                output += '<sub>'
                                output += parse_str_or_lst(v)
                                output += '</sub>'
                            elif k == 'sis:YleinenViite':
                                output += parse_links_dict_or_list(v)
                            elif k == 'sis:AlaviiteKooste':
                                output += parse_formatted_text(v)
                            else:
                                import pdb;pdb.set_trace()
                                raise IncompleteDecisionTreeException
                    elif isinstance(section, list):
                        for item in section:
                            output += parse_formatted_text(item)
                    elif section is None:
                        return output # Don't want to add new lines to the output
                    else:
                        raise IncompleteDecisionTreeException
                    output += "\n\n"
                    return output

                # Recursively parse out markdown from nested xml
                if isinstance(xml, str):
                    output = xml
                elif 'asi:PerusteluLuku' in xml.keys():  # Perusteluluvuissa useita osioita --> lista
                    output = parse_sub_title(xml, level)
                    for section in xml['asi:PerusteluLuku']:
                        parse_asi_or_sis(section, level+1)
                elif 'sis:KappaleKooste' in xml.keys():  # Kappalekoosteissa tekstikappaleita
                    output = parse_sub_title(xml, level)
                    if isinstance(xml['sis:KappaleKooste'], list):
                        for section in xml['sis:KappaleKooste']:
                            output += parse_formatted_text(section)
                elif 'sis:LukuOtsikko' in xml.keys():
                    output = parse_sub_title(xml, level)
                else:
                    raise IncompleteDecisionTreeException
                return output

            bp_brief_out = bp_id['met:Nimeke']['met1:NimekeTeksti'] + "\n\n"
            if bp_brief['sis1:OtsikkoTeksti'] is not None:
                bp_brief_out += "#" + bp_brief['sis1:OtsikkoTeksti']
            for section in bp_brief['sis:KappaleKooste']:
                if isinstance(section, dict):
                    section = section['#text']
                bp_brief_out += "\n\n" + section
            if isinstance(bp_full, list):
                bp_full_out = ""
                for section in bp_full:
                    bp_full_out += parse_asi_or_sis(section)
            else:
                bp_full_out = parse_asi_or_sis(bp_full)

            return bp_brief_out, bp_full_out

        xml = xml['ns11:Siirto']
        if 'SiirtoAsiakirja' in xml.keys():
            xml = xml['SiirtoAsiakirja']
            bp_identifiointiosa = xml['RakenneAsiakirja']['he:HallituksenEsitys']['asi:IdentifiointiOsa']
            bp_brief = xml['RakenneAsiakirja']['he:HallituksenEsitys']['asi:SisaltoKuvaus']
            bp_full = xml['RakenneAsiakirja']['he:HallituksenEsitys']['asi:PerusteluOsa']
            bp_brief, bp_full = parse_with_identifiointiosa(bp_identifiointiosa, bp_brief, bp_full)
        elif 'ns:SiirtoAsiakirja' in xml.keys():
            xml = xml['ns:SiirtoAsiakirja']
            bp_identifiointiosa = xml['ns:RakenneAsiakirja']['he:HallituksenEsitys']['asi:IdentifiointiOsa']
            bp_brief = xml['ns:RakenneAsiakirja']['he:HallituksenEsitys']['asi:SisaltoKuvaus']
            bp_full = xml['ns:RakenneAsiakirja']['he:HallituksenEsitys']['asi:PerusteluOsa']
            bp_brief, bp_full = parse_with_identifiointiosa(bp_identifiointiosa, bp_brief, bp_full)
        elif 'ns11:SiirtoMetatieto' in xml.keys():
            xml = xml['ns11:SiirtoMetatieto']
            bp_full = ""
            if 'jme:JulkaisuMetatieto' in xml.keys():
                bp_brief = xml['jme:JulkaisuMetatieto']['asi:IdentifiointiOsa']['met:Nimeke']['met1:NimekeTeksti']
            elif 'ns:JulkaisuMetatieto' in xml.keys():
                bp_brief = xml['ns:JulkaisuMetatieto']['ns:IdentifiointiOsa']['ns:Nimeke']['ns:NimekeTeksti']['#text']
            else:
                raise IncompleteDecisionTreeException
        else:
            raise IncompleteDecisionTreeException        
        

        return bp_brief, bp_full

    bp_brief, bp_full = find_relevant_xml(data)
    
    return bp_brief, bp_full


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
