import os.path
import pandas as pd
from lxml import etree
from io import StringIO
from XML_parsing_help_functions import date_parse, rollcall_id_parse, NS

from db import get_connection

class IncompleteDecisionTreeException(Exception):
    pass

speeches_csv_path = os.path.join("data", "preprocessed", "speeches.csv")
records_csv_path = os.path.join("data", "preprocessed", "records.csv")
agenda_items_csv_path = os.path.join("data", "preprocessed", "agenda_items.csv")

def preprocess_data():
    # Load the TSV file
    df_tsv = pd.read_csv(os.path.join("data", "raw", "vaski", "Record_fi.tsv"), sep="\t")

    records = []
    agenda_items = []
    speeches_list = []

    for xml_str in df_tsv['XmlData']:
        root = etree.parse(StringIO(xml_str)).getroot()
        # Get parliament_id
        p_id = root.xpath(".//asi:EduskuntaTunniste", namespaces=NS)
        p_type = p_id[0].findtext("met1:AsiakirjaTyyppiTeksti", namespaces=NS)
        if p_type is None:
            p_type = p_id[0].findtext("met1:AsiakirjatyyppiKoodi", namespaces=NS)
        # The general assembly code is PTK, committees have committee abbreviation + P
        p_type = p_type[:-1] if p_type.endswith("P") else "EK"
        p_number = p_id[0].findtext("asi1:AsiakirjaNroTeksti", namespaces=NS)
        p_year = p_id[0].findtext("asi1:ValtiopaivavuosiTeksti", namespaces=NS)


        laadinta_pvm = date_parse(root, NS)
        ptk_element = root.find(".//ptk:KokousPoytakirja", namespaces=NS) 
        if ptk_element is None:
            ptk_element = root.find(".//ptk:Poytakirja", namespaces=NS)
        if ptk_element is None:  # Should never be None after that
            raise IncompleteDecisionTreeException(msg="ptk_element is None when it should not be None")
        try:
            kokous_pvm = ptk_element.attrib.get(f'{{{NS['vsk1']}}}kokousAloitusHetki')[:10]
        except TypeError:
            if p_number == '107' and p_year == '2018':
                # The data has a row that falls to this due to improper document structure.
                kokous_pvm = laadinta_pvm
            else:
                raise IncompleteDecisionTreeException

        # In case the meeting was a parliament plenary session, fetch the rollcall of the meeting
        if p_type == "EK":
            rollcall_id = rollcall_id_parse(root)
        else:
            rollcall_id = None

        records.append({
            "assembly_code": p_type,
            "number": p_number,
            "year": p_year,
            "meeting_date": kokous_pvm,
            "creation_date": laadinta_pvm,
            "rollcall_id": rollcall_id})

        # Find speeches
        asiakohdat = root.xpath(".//vsk:Asiakohta", namespaces=NS)
        for asiakohta in asiakohdat:
            asiakohta_otsikko = asiakohta.find(".//vsk:KohtaNimeke/met1:NimekeTeksti", namespaces=NS).text
            agenda_item_parliament_id = asiakohta.get('{http://www.vn.fi/skeemat/metatietoelementit/2010/04/27}eduskuntaTunnus')
            if agenda_item_parliament_id is None:
                agenda_item_parliament_id = asiakohta.get('{http://www.vn.fi/skeemat/metatietoelementit/2010/04/27}muuTunnus')
            agenda_item_parliament_id = agenda_item_parliament_id.lower()
            agenda_items.append({
                "record_assembly_code": p_type,
                "record_year": p_year,
                "record_number": p_number,
                "parliament_id": agenda_item_parliament_id,
                "title": asiakohta_otsikko
            })
            speeches = asiakohta.xpath(".//vsk:PuheenvuoroToimenpide", namespaces=NS)
            for speech in speeches:
                speaker = speech.find(".//org:Henkilo", namespaces=NS)
                speaker_id = speaker.get(f"{{{NS['met1']}}}muuTunnus") if speaker is not None else None
                speech_type = speech.get(f"{{{NS['vsk1']}}}puheenvuoroLuokitusKoodi")
                speech_id_tag = speech.find(".//vsk:PuheenvuoroOsa", namespaces=NS)
                speech_id = speech_id_tag.get(f"{{{NS['met1']}}}muuTunnus") if speech_id_tag is not None else None

                start_time = speech.get(f"{{{NS['vsk1']}}}puheenvuoroAloitusHetki")
                if start_time:
                    start_time = start_time.replace("T", " ") + " Europe/Helsinki"

                # Build speech text
                body_parts = []
                # Extract regular speech paragraphs
                paragraphs = speech.xpath(".//vsk:PuheenvuoroOsa//sis:KappaleKooste", namespaces=NS)
                for para in paragraphs:
                    text = para.text.strip() if para.text else ""
                    if text:
                        body_parts.append(text)

                # Append puhemies interventions (separately)
                interventions = speech.xpath(".//vsk:PuheenjohtajaRepliikki", namespaces=NS)
                for intervention in interventions:
                    chair_text = intervention.findtext(".//vsk1:PuheenjohtajaTeksti", namespaces=NS)
                    chair_paragraphs = intervention.findall(".//sis:KappaleKooste", namespaces=NS)
                    for para in chair_paragraphs:
                        ptext = para.text.strip() if para.text else ""
                        if chair_text and ptext:
                            body_parts.remove(ptext) # Remove duplicate chair text
                            body_parts.append(f"**{chair_text}**: {ptext}")

                full_text = "\n\n".join(body_parts)
                
                if speaker_id:
                    if speaker_id.strip():
                        role = speech.find(".//org1:AsemaTeksti", namespaces=NS)
                        if role != None:
                            if "ministeri" not in role.text:
                                continue
                        
                        # There are duplicates in speech ids.
                        # Add year to the front of speech id to fix issue
                        speech_id = start_time[:4] + "/" + speech_id
                        if speech.find(".//vsk1:TarkenneTeksti", namespaces=NS) is not None and \
                            speech.find(".//vsk1:TarkenneTeksti", namespaces=NS).text == "(vastauspuheenvuoro)":
                            response_to = root_id
                        else:
                            response_to = speech_id
                            root_id = speech_id
                        speeches_list.append({
                            "speech_id": speech_id,
                            "speaker_id": speaker_id,
                            "record_assembly_code": p_type,
                            "record_number": p_number,
                            "record_year": p_year,
                            "agenda_item_parliament_id": agenda_item_parliament_id,
                            "start_time": start_time,
                            "speech_text": full_text,
                            "speech_type": speech_type,
                            "response_to": response_to
                        })


    # Convert to DataFrame
    df_speeches = pd.DataFrame(speeches_list)
    df_agenda_items = pd.DataFrame(agenda_items)
    df_records = pd.DataFrame(records)

    # Optional: Save to CSV
    df_speeches.to_csv(speeches_csv_path, index=False, encoding="utf-8")

    # records
    df_records.drop_duplicates(inplace=True)
    df_records.to_csv(records_csv_path, index=False)

    # For unknown reasons, the agenda items are presented multiple times.
    # It doesn't matter as long as the references are to the correct title
    df_agenda_items.title = df_agenda_items.title.str.strip()
    df_agenda_items.title = df_agenda_items.title.apply(lambda x: " ".join(x.split()))
    df_agenda_items.title = df_agenda_items.title.str.replace("-—", "-")
    df_agenda_items.drop_duplicates(inplace=True)
    # Datassa on yksi rivi, jossa otsikko on eri kuin koodien antaisi ymmärtää. Pudotetaan tämä
    df_agenda_items = df_agenda_items[~(df_agenda_items.title == "Tilapäisen puheenjohtajan valinta")]
    df_agenda_items.to_csv(agenda_items_csv_path, index=False)

def import_data():
    conn = get_connection()
    cursor = conn.cursor()

    with open(records_csv_path) as f:
        cursor.copy_expert("COPY records(assembly_code, number, year, meeting_date, creation_date, rollcall_id) FROM stdin DELIMITERS ',' CSV HEADER QUOTE '\"';", f)

    with open(agenda_items_csv_path) as f:
        cursor.copy_expert("COPY agenda_items(record_assembly_code, record_year, record_number, parliament_id, title) FROM stdin DELIMITERS ',' CSV HEADER QUOTE '\"';", f)

    with open(speeches_csv_path) as f:
        cursor.copy_expert("COPY speeches(id, person_id, record_assembly_code, record_number, record_year, agenda_item_parliament_id, start_time, speech, speech_type, response_to) FROM stdin DELIMITERS ',' CSV HEADER QUOTE '\"';", f)


    conn.commit()
    cursor.close()
    conn.close()

if __name__ == '__main__':
    import argparse
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
        
