import os.path
from lxml import etree
import csv
import psycopg2
from harmonize import harmonize_party

csv_path = 'data/preprocessed/mp_party_memberships.csv'


def preprocess_data():
    rows = []
    with open(os.path.join("data", "raw", "MemberOfParliament.tsv")) as f:
        mp_data = [row for row in csv.reader(f, delimiter="\t", quotechar='"')]

    for mp in mp_data[1:]:
        mp_id = int(mp[0])

        xml = mp[7]
        root = etree.fromstring(xml)
        namespaces = root[0].nsmap

        # Current group
        cur_group = root.find("./Eduskuntaryhmat/NykyinenEduskuntaryhma", namespaces)
        if cur_group is not None and cur_group[0] is not None and cur_group[0].text is not None:
            party = harmonize_party(cur_group[0].text)
            AlkuPvm = cur_group.find("./AlkuPvm", namespaces)
            start_date = "-".join(list(reversed((AlkuPvm.text).split("."))))
            end_date = None

            rows.append({
                "mp_id": mp_id,
                "party_id": party,
                "start_date": start_date,
                "end_date": end_date
            })

        for group in root.find("./Eduskuntaryhmat/EdellisetEduskuntaryhmat", namespaces):
            if group[0].text:
                party = harmonize_party(group[0].text)
                
                for membership in group.findall("./Jasenyys", namespaces):
                    AlkuPvm = membership.find("./AlkuPvm", namespaces)
                    start_date = "-".join(list(reversed((AlkuPvm.text).split("."))))
                    
                    LoppuPvm = membership.find("./LoppuPvm", namespaces)
                    end_date = "-".join(list(reversed((LoppuPvm.text).split("."))))

                    rows.append({
                        "mp_id": mp_id,
                        "party_id": party,
                        "start_date": start_date,
                        "end_date": end_date
                    })

    with open(csv_path, 'w') as f:
        writer = csv.DictWriter(f, fieldnames=["mp_id", "party_id", "start_date", "end_date"])
        writer.writeheader()
        writer.writerows(rows)


def import_data():
    conn = psycopg2.connect(database="postgres",
                            host="db",
                            user="postgres",
                            password="postgres",
                            port="5432")
    cursor = conn.cursor()

    with open(csv_path) as f:
        cursor.copy_expert("COPY mp_party_memberships(mp_id, party_id, start_date, end_date) FROM stdin DELIMITERS ',' CSV HEADER QUOTE '\"';", f)

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
