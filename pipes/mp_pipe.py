import os
import pandas as pd
import xml.etree.ElementTree as ET
import csv
import psycopg2


csv_path = 'data/preprocessed/members_of_parliament.csv'


def preprocess_data():
    with open(os.path.join("data", "raw", "MemberOfParliament.tsv")) as f:
        df = pd.read_csv(f, sep="\t")

    photo_filenames = os.listdir("frontend/src/assets/")
    photo_filename_dict = {
        filename.split(".")[0].split("-")[-1]: filename for filename in photo_filenames
    }

    rows = []
    for mp in df.iterrows():
        mp = mp[1]

        id = mp['personId']

        minister = True if mp['minister'] == 't' else False

        xml = mp['XmlDataFi']
        tree = ET.fromstring(xml)

        row = {
            "id": id,
            "first_name": mp['firstname'].strip(),
            "last_name": mp['lastname'].strip(),
            "full_name": f"{tree[1].text} {tree[2].text}",
            "minister": minister,
            "phone_number": tree[6].text,
            "email": tree[7].text,
            "occupation": tree[9].text,
            "year_of_birth": tree[10].text,
            "place_of_birth": tree[11].text,
            "place_of_residence": tree[15].text,
            "constituency": tree[26][0][0].text,
            "photo": photo_filename_dict[str(id)] if str(id) in photo_filename_dict else None
        }

        rows.append(row)

    with open(csv_path, 'w') as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writerows(rows)
        
def import_data():
    conn = psycopg2.connect(database="postgres",
                            host="db",
                            user="postgres",
                            password="postgres",
                            port="5432")
    cursor = conn.cursor()

    with open(csv_path) as f:
        cursor.copy_expert("COPY members_of_parliament(id, first_name, last_name, full_name, minister, phone_number, email, occupation, year_of_birth, place_of_birth, place_of_residence, constituency, photo) FROM stdin DELIMITERS ',' CSV QUOTE '\"';", f)

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
        import_data()
        preprocess_data()

