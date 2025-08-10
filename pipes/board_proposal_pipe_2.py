#!/usr/bin/env python3
"""
parse_proposals_to_markdown_tables_fixed.py

Improved table detection:
- handles explicit <table>/<tr>/<td>/<entry> structures
- infers tables from sequences of paragraph-like nodes or entry nodes by grouping
  consecutive leaf texts into rows (tries 2-4 columns heuristics)

Reads data/raw/vaski/GovernmentProposal_fi.tsv (XmlData column),
parses to Markdown, truncates the output table, writes to PostgreSQL.
Testing mode (first 10 rows) is enabled by default.
"""
import re
from typing import List, Tuple, Iterable, Optional
import pandas as pd
from psycopg2.extras import execute_values
import psycopg2

# Prefer defusedxml for safety; fall back to stdlib
try:
    from defusedxml import ElementTree as ET
except Exception:
    import xml.etree.ElementTree as ET

# ---------- Configuration ----------
TSV_PATH = "data/raw/vaski/GovernmentProposal_fi.tsv"
XML_COLUMN = "XmlData"
ID_COLUMN = "Id"

TESTING = True
TEST_ROWS = 10
CHUNKSIZE = 500

DB_CONFIG = {
    "dbname": "postgres",
    "user": "postgres",
    "password": "postgres",
    "host": "db",
    "port": 5432
}

# ---------- Utilities ----------
_ns_strip = re.compile(r"\{.*\}")

def localname(tag: Optional[str]) -> str:
    if tag is None:
        return ""
    return _ns_strip.sub("", tag).lower()

def clean_text(t: Optional[str]) -> str:
    if t is None:
        return ""
    return re.sub(r"\s+", " ", t).strip()

# ---------- Parsing config ----------
TITLE_KEYWORDS = ["otsikko", "saadostyyppinimi", "aihe", "saadosnimeke",
                  "lukuotsikko", "saadosotsikko", "otsikkoteksti", "asiakirjatyyppinimi"]
PARAGRAPH_TAGS = set([
    "kappale", "kappalekooste", "perusteluluku", "perustelu", "tekstikooste",
    "teksti", "paragraph", "entry", "johtolause", "perusteluteksti"
])
INLINE_ITALIC = {"kursiiviteksti", "kursiivinen"}
INLINE_BOLD = {"lihavateksti", "lihavoitu"}
TABLE_TAGS = {"table", "taulukkokooste", "taulukko"}
ROW_TAGS = {"row", "rivi", "tr"}
CELL_TAGS = {"entry", "cell", "td", "th", "solu", "col"}
NUMERIC_PREFIX_RE = re.compile(r'^\s*(\d+(?:\.\d+)*\.)\s*(.*)$')

# ---------- Inline rendering ----------
def recurse_child(c) -> str:
    tagln = localname(c.tag)
    txt = clean_text(c.text)
    if tagln in INLINE_BOLD:
        inner = [txt] if txt else []
        for gc in c:
            inner.append(recurse_child(gc))
        combined = " ".join([p for p in inner if p]).strip()
        return f"**{combined}**" if combined else ""
    elif tagln in INLINE_ITALIC:
        inner = [txt] if txt else []
        for gc in c:
            inner.append(recurse_child(gc))
        combined = " ".join([p for p in inner if p]).strip()
        return f"*{combined}*" if combined else ""
    else:
        parts = [txt] if txt else []
        for gc in c:
            parts.append(recurse_child(gc))
        tail = clean_text(c.tail)
        if tail:
            parts.append(tail)
        return " ".join([p for p in parts if p]).strip()

def inline_text(elem) -> str:
    pieces: List[str] = []
    def recurse(e):
        tagln = localname(e.tag)
        t = clean_text(e.text)
        if tagln in INLINE_BOLD:
            inner_parts = [t] if t else []
            for c in e:
                inner_parts.append(recurse_child(c))
            combined = " ".join([p for p in inner_parts if p]).strip()
            if combined:
                pieces.append("**" + combined + "**")
        elif tagln in INLINE_ITALIC:
            inner_parts = [t] if t else []
            for c in e:
                inner_parts.append(recurse_child(c))
            combined = " ".join([p for p in inner_parts if p]).strip()
            if combined:
                pieces.append("*" + combined + "*")
        else:
            if t:
                pieces.append(t)
            for c in e:
                pieces.append(recurse_child(c))
        if e.tail:
            tail = clean_text(e.tail)
            if tail:
                pieces.append(tail)
        return ""
    recurse(elem)
    return " ".join([p for p in pieces if p]).strip()

# ---------- Improved table detection & conversion ----------
def _gather_leaf_texts_under(elem) -> List[str]:
    """
    Returns a list of cleaned text snippets from leaf nodes (text that
    is not further split into child elements). Excludes empty strings.
    Preserves document order.
    """
    texts = []
    # If element has direct text and no element children, include it
    if (not list(elem)) and elem.text:
        t = clean_text(elem.text)
        if t:
            texts.append(t)
    # walk descendants in document order and collect text from nodes that lack element children
    for descendant in elem.iter():
        if not isinstance(descendant.tag, str):
            continue
        if list(descendant):  # has element children -> not leaf
            continue
        # collect either text or tail if present
        txt = clean_text(descendant.text)
        if txt:
            texts.append(txt)
        # note: descendant.tail will be collected by its parent iteration typically
    return texts

def _best_column_count_for_texts(texts: List[str], min_cols=2, max_cols=4):
    """
    Try to pick a sensible column count for grouping consecutive texts into rows.
    Preference: pick smallest column count that results in few leftover cells.
    Returns chosen column count or None if none looks good.
    """
    if not texts:
        return None
    n = len(texts)
    best = None
    best_score = None
    for cols in range(min_cols, max_cols+1):
        rem = n % cols
        # score: prefer zero remainder, then small remainder; penalize higher cols slightly
        score = (rem) + 0.1 * cols
        if best_score is None or score < best_score:
            best_score = score
            best = cols
    # require that there are at least 2 rows with this column count (avoid spuriously grouping 1 item into single row)
    if n // best < 2:
        return None
    return best

def table_from_unstructured(elem) -> Optional[str]:
    """
    Heuristic fallback: try to infer a table from an element that is not
    explicitly a <table>. Returns markdown table string or None.
    """
    # Gather leaf texts under the element
    texts = _gather_leaf_texts_under(elem)
    if len(texts) < 4:
        return None

    # If there's a colspec-like hint (e.g., child named 'colspec'), attempt to read a number
    colspecs = [c for c in elem if isinstance(c.tag, str) and 'colspec' in localname(c.tag)]
    if colspecs:
        # naive: count colspec children or split attribute 'cols' if exists
        cols = len(colspecs)
    else:
        cols = _best_column_count_for_texts(texts)
    if not cols or cols < 1:
        return None

    # Build rows by grouping consecutive texts
    rows = []
    i = 0
    while i < len(texts):
        row = texts[i:i+cols]
        # if last row short, pad
        if len(row) < cols:
            row = row + [''] * (cols - len(row))
        rows.append(row)
        i += cols

    # If too few rows, bail
    if len(rows) < 2:
        return None

    # Create markdown: first row -> header, then separator, then rest
    max_cols = cols
    md_lines = []
    header = rows[0]
    md_lines.append("| " + " | ".join(h or "" for h in header) + " |")
    md_lines.append("| " + " | ".join(["---"] * max_cols) + " |")
    for r in rows[1:]:
        md_lines.append("| " + " | ".join(cell or "" for cell in r) + " |")
    return "\n".join(md_lines)

def table_to_markdown(elem) -> str:
    """
    Convert to markdown table, but only if it meets minimum shape criteria.
    Avoids false positives where row/entry are used for inline layout.
    """
    rows = []
    for r in elem.findall(".//*"):
        if localname(r.tag) in ROW_TAGS:
            cells = [inline_text(c) for c in r if localname(c.tag) in CELL_TAGS]
            if cells:
                rows.append(cells)

    # Check if it looks like a real table
    if len(rows) < 2:
        return ""  # too few rows
    if max(len(r) for r in rows) < 2:
        return ""  # too few columns

    # Normalize column counts
    max_cols = max(len(r) for r in rows)
    rows = [r + [""] * (max_cols - len(r)) for r in rows]

    # Build markdown
    md_lines = []
    header = rows[0]
    md_lines.append("| " + " | ".join(header) + " |")
    md_lines.append("| " + " | ".join(["---"] * max_cols) + " |")
    for r in rows[1:]:
        md_lines.append("| " + " | ".join(r) + " |")
    return "\n".join(md_lines)

# ---------- XML -> parts (uses new table detection) ----------
def xml_to_parts(xml_str: str) -> Tuple[List[Tuple[str, str, Optional[int]]], str]:
    if not xml_str or not xml_str.strip():
        return [], ""
    try:
        root = ET.fromstring(xml_str)
    except Exception:
        try:
            root = ET.fromstring(f"<root>{xml_str}</root>")
        except Exception:
            return [], ""

    parts: List[Tuple[str, str, Optional[int]]] = []
    hashtags = ""

    # --- Collect hashtags ---
    aihe_texts = []
    aiheteksti_elems = set()
    for elem in root.iter():
        if localname(elem.tag) == "aiheteksti":
            t = clean_text(elem.text)
            if t:
                aihe_texts.append(t)
                aiheteksti_elems.add(elem)
    if aihe_texts:
        hashtags = " ".join(f"#{t.replace(' ', '_')}" for t in aihe_texts)

    # --- Normal processing ---
    for el in root.iter():
        if el in aiheteksti_elems:
            continue  # skip these, already captured

        tagln = localname(el.tag)
        children = [c for c in list(el) if isinstance(c.tag, str)]
        has_children = len(children) > 0
        direct_text = clean_text(el.text)

        if tagln in INLINE_BOLD or tagln in INLINE_ITALIC:
            txt = inline_text(el)
            if txt:
                parts.append(("paragraph", txt, None))
            continue

        if any(k in tagln for k in TITLE_KEYWORDS):
            if (not has_children) or (direct_text):
                txt = inline_text(el)
                if txt:
                    if "luku" in tagln or "saados" in tagln:
                        parts.append(("heading", "## " + txt, None))
                    elif "pykala" in tagln:
                        parts.append(("paragraph", f"**{txt}**", None))
                    else:
                        parts.append(("heading", "# " + txt, None))
            continue

        if tagln in TABLE_TAGS or tagln == "table":
            md = table_to_markdown(el)
            if md:
                parts.append(("table", md, None))
            continue

        if tagln in PARAGRAPH_TAGS:
            if (not has_children) or (direct_text):
                txt = inline_text(el)
                if txt:
                    m = NUMERIC_PREFIX_RE.match(txt)
                    if m:
                        prefix = m.group(1)
                        rest = m.group(2).strip()
                        level = prefix.count('.') or 1
                        content = rest if rest else txt
                        parts.append(("list_item", content, level))
                    else:
                        parts.append(("paragraph", txt, None))
            continue

        if has_children:
            paragraph_children = [c for c in children if localname(c.tag) in PARAGRAPH_TAGS or not list(c)]
            if len(paragraph_children) >= 4:
                md = table_to_markdown(el)
                if md:
                    parts.append(("table", md, None))
                    continue

    return parts, hashtags


# ---------- Render into markdown (no dedup) ----------
def render_parts(parts: List[Tuple[str, str, Optional[int]]]) -> str:
    out_blocks: List[str] = []
    i = 0
    while i < len(parts):
        typ, content, *rest = parts[i]
        level = rest[0] if rest else None
        if typ == "list_item":
            j = i
            list_lines: List[str] = []
            while j < len(parts) and parts[j][0] == "list_item":
                _, cont, lvl = parts[j]
                lvl = lvl or 1
                indent = " " * ((lvl - 1) * 4)
                list_lines.append(f"{indent}1. {cont}")
                j += 1
            out_blocks.append("\n".join(list_lines))
            i = j
        else:
            out_blocks.append(content)
            i += 1
    return "\n\n".join([b for b in out_blocks if b and b.strip()]).strip()

# ---------- DB helpers ----------
def ensure_table(cursor):
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS proposals_markdown (
        id SERIAL PRIMARY KEY,
        proposal_id TEXT,
        markdown TEXT,
        hashtags TEXT
    );
    """)

def clear_output_table():
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        cur = conn.cursor()
        ensure_table(cur)
        cur.execute("TRUNCATE TABLE proposals_markdown;")
        conn.commit()
        cur.close()
    finally:
        conn.close()

def write_batch_to_db(records: Iterable[Tuple[str, str, str]]):
    if not records:
        return
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        cur = conn.cursor()
        ensure_table(cur)
        insert_sql = "INSERT INTO proposals_markdown (proposal_id, markdown, hashtags) VALUES %s"
        execute_values(cur, insert_sql, list(records), page_size=100)
        conn.commit()
    finally:
        conn.close()

# ---------- Main ----------
def process_first_n(tsv_path: str, n: int):
    df = pd.read_csv(tsv_path, sep="\t", dtype=str, keep_default_na=False, nrows=n, encoding='utf-8')
    rows_to_insert: List[Tuple[str, str]] = []
    for idx, row in df.iterrows():
        proposal_id = row.get(ID_COLUMN) or str(idx)
        xml = row.get(XML_COLUMN, "")
        parts, hashtags = xml_to_parts(xml)
        md = render_parts(parts)
        rows_to_insert.append((proposal_id, md, hashtags))
    if rows_to_insert:
        write_batch_to_db(rows_to_insert)
    print(f"Inserted {len(rows_to_insert)} rows (testing mode)")

def process_all_chunked(tsv_path: str, chunksize: int = CHUNKSIZE):
    total_processed = 0
    df_iter = pd.read_csv(tsv_path, sep="\t", dtype=str, keep_default_na=False, chunksize=chunksize, encoding='utf-8')
    for chunk in df_iter:
        rows_to_insert: List[Tuple[str, str]] = []
        for idx, row in chunk.iterrows():
            proposal_id = row.get(ID_COLUMN) or str(idx)
            xml = row.get(XML_COLUMN, "")
            parts = xml_to_parts(xml)
            md = render_parts(parts)
            rows_to_insert.append((proposal_id, md))
        if rows_to_insert:
            write_batch_to_db(rows_to_insert)
        total_processed += len(rows_to_insert)
        print(f"Inserted {len(rows_to_insert)} rows (total so far: {total_processed})")
    print("Done. Total rows processed:", total_processed)

if __name__ == "__main__":
    print("Clearing existing data in proposals_markdown...")
    clear_output_table()
    print("Table cleared.")
    if TESTING:
        print(f"TESTING mode: processing only first {TEST_ROWS} rows from {TSV_PATH}")
        process_first_n(TSV_PATH, TEST_ROWS)
    else:
        print(f"Processing entire file (chunked) from {TSV_PATH}")
        process_all_chunked(TSV_PATH)
