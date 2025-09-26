import os.path
import psycopg2
import pandas as pd
from lxml import etree
from io import StringIO
from tqdm import tqdm

from db import get_connection

csv_path = os.path.join("data", "preprocessed", "speeches.csv")

def preprocess_data():
    # Load the TSV file
    df_tsv = pd.read_csv(os.path.join("data", "raw", "vaski", "Record_fi.tsv"), sep="\t")

    # Define namespaces
    ns = {
        "asi": "http://www.vn.fi/skeemat/asiakirjakooste/2010/04/27",
        "asi1": "http://www.vn.fi/skeemat/asiakirjaelementit/2010/04/27",
        "met1": "http://www.vn.fi/skeemat/metatietoelementit/2010/04/27",
        "vsk": "http://www.eduskunta.fi/skeemat/vaskikooste/2011/01/04",
        "vsk1": "http://www.eduskunta.fi/skeemat/vaskielementit/2011/01/04",
        "sis": "http://www.vn.fi/skeemat/sisaltokooste/2010/04/27",
        "org": "http://www.vn.fi/skeemat/organisaatiokooste/2010/02/15",
        "org1": "http://www.vn.fi/skeemat/organisaatioelementit/2010/02/15",
        "met": "http://www.vn.fi/skeemat/metatietokooste/2010/04/27",
    }

    parsed_data = []

    for xml_str in tqdm(df_tsv['XmlData']):
        root = etree.parse(StringIO(xml_str)).getroot()

        # Get parliament_id
        p_id = root.xpath(".//asi:EduskuntaTunniste", namespaces=ns)
        if not p_id:
            continue

        p_type = p_id[0].findtext("asi1:AsiakirjaNroTeksti", namespaces=ns)
        p_year = p_id[0].findtext("asi1:ValtiopaivavuosiTeksti", namespaces=ns)
        if not (p_type and p_year):
            continue

        parliament_id = f"ptk {p_type}/{p_year.replace(' vp','')} vp"

        # Find speeches
        speeches = root.xpath(".//vsk:PuheenvuoroToimenpide", namespaces=ns)
        for speech in speeches:
            speaker = speech.find(".//org:Henkilo", namespaces=ns)
            speaker_id = speaker.get(f"{{{ns['met1']}}}muuTunnus") if speaker is not None else None

            speech_id_tag = speech.find(".//vsk:PuheenvuoroOsa", namespaces=ns)
            speech_id = speech_id_tag.get(f"{{{ns['met1']}}}muuTunnus") if speech_id_tag is not None else None

            start_time = speech.get(f"{{{ns['vsk1']}}}puheenvuoroAloitusHetki")
            if start_time:
                start_time = start_time.replace("T", " ") + " Europe/Helsinki"

            # Build speech text
            body_parts = []
            # Extract regular speech paragraphs
            paragraphs = speech.xpath(".//vsk:PuheenvuoroOsa//sis:KappaleKooste", namespaces=ns)
            for para in paragraphs:
                text = para.text.strip() if para.text else ""
                if text:
                    body_parts.append(text)

            # Append puhemies interventions (separately)
            interventions = speech.xpath(".//vsk:PuheenjohtajaRepliikki", namespaces=ns)
            for intervention in interventions:
                chair_text = intervention.findtext(".//vsk1:PuheenjohtajaTeksti", namespaces=ns)
                chair_paragraphs = intervention.findall(".//sis:KappaleKooste", namespaces=ns)
                for para in chair_paragraphs:
                    ptext = para.text.strip() if para.text else ""
                    if chair_text and ptext:
                        body_parts.append(f"**{chair_text}**: {ptext}")

            full_text = "\n\n".join(body_parts)
            
            if speaker_id:
                if speaker_id.strip():
                    role = speech.find(".//org1:AsemaTeksti", namespaces=ns)
                    if role != None:
                        if "ministeri" not in role.text:
                            continue
                    
                    # There are duplicates in speech ids.
                    # Add year to the front of speech id to fix issue
                    speech_id = start_time[:4] + "/" + speech_id
                    if speech.find(".//vsk1:TarkenneTeksti", namespaces=ns) is not None and \
                        speech.find(".//vsk1:TarkenneTeksti", namespaces=ns).text == "(vastauspuheenvuoro)":
                        response_to = root_id
                    else:
                        response_to = None
                        root_id = speech_id
                    parsed_data.append({
                        "speech_id": speech_id,
                        "speaker_id": speaker_id,
                        "parliament_id": parliament_id,
                        "start_time": start_time,
                        "speech_text": full_text,
                        "response_to": response_to
                    })

    # Convert to DataFrame
    df_speeches = pd.DataFrame(parsed_data)

    # Optional: Save to CSV
    df_speeches.to_csv(csv_path, index=False, encoding="utf-8")

def import_data():
    conn = get_connection()
    cursor = conn.cursor()

    with open(csv_path) as f:
        cursor.copy_expert("COPY speeches(id, person_id, parliament_id, start_time, speech, response_to) FROM stdin DELIMITERS ',' CSV HEADER QUOTE '\"';", f)

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
        
