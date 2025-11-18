import os
import json
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()

DATA_FOLDER = "fundamentals_data/nse"
HOST = os.getenv("HOST")
DATABASE = os.getenv("DATABASE")
USER = os.getenv("USER")
PASSWORD = os.getenv("PASSWORD")

# -----------------------------------------------
# Postgres Connection
# -----------------------------------------------
conn = psycopg2.connect(
    host=HOST,
    database=DATABASE,
    user=USER,
    password=PASSWORD
)
cursor = conn.cursor()

FINANCIAL_BLOCKS = [
    "per_share_data_array",
    "common_size_ratios",
    "income_statement",
    "balance_sheet",
    "cashflow_statement",
    "valuation_ratios",
    "valuation_and_quality",
]


# ---------------------------------------------------------
# 1. Extract all unique metric names first (2-pass process)
# ---------------------------------------------------------
def scan_all_metric_names():
    metrics = {block: set() for block in FINANCIAL_BLOCKS}

    for filename in os.listdir(DATA_FOLDER):
        if not filename.endswith(".json"):
            continue

        path = os.path.join(DATA_FOLDER, filename)
        with open(path, "r") as f:
            data = json.load(f)

        if "financials" not in data:
            continue

        financials = data["financials"]

        for period_type in ["annuals", "quarterly"]:
            if period_type not in financials:
                continue

            period_block = financials[period_type]

            for block_name in FINANCIAL_BLOCKS:
                if block_name in period_block:
                    block_data = period_block[block_name]
                    for metric_key in block_data.keys():
                        metrics[block_name].add(metric_key)

    return metrics


# ---------------------------------------------------------
# 2. Insert metric rows into fundamental_data_type
# ---------------------------------------------------------
def create_fundamental_data_type_rows(metrics):
    """
    Insert if missing. Return mapping:
    mapping[block_name][metric_name] = id
    """
    cursor.execute("SELECT type, name, id FROM fundamental_data_type;")
    existing = {(row[0], row[1]): row[2] for row in cursor.fetchall()}

    mapping = {block: {} for block in FINANCIAL_BLOCKS}

    for block, metric_names in metrics.items():
        for metric in metric_names:

            if (block, metric) in existing:
                mapping[block][metric] = existing[(block, metric)]
                continue

            cursor.execute(
                "INSERT INTO fundamental_data_type (type, name) VALUES (%s, %s) RETURNING id;",
                (block, metric)
            )
            mapping[block][metric] = cursor.fetchone()[0]

    conn.commit()
    return mapping


# ---------------------------------------------------------
# Helpers
# ---------------------------------------------------------
def valid_number(v):
    if v in ["N/A", "-", "", None]:
        return None
    try:
        return float(v)
    except:
        return None

def extract_year_month(fy):
    if fy is None or fy == "" or fy == "TTM" or "-" not in fy:
        return None, None
    year, month = fy.split("-")[:2]
    return year, month


# ---------------------------------------------------------
# 3. Convert each file into rows referencing correct metric ID
# ---------------------------------------------------------
def process_file(filepath, mapping):
    rows = []
    ticker = os.path.basename(filepath).replace(".json", "")

    with open(filepath, "r") as f:
        data = json.load(f)

    if "financials" not in data:
        return rows

    financials = data["financials"]

    for period_type in ["annuals", "quarterly"]:
        if period_type not in financials:
            continue

        period_block = financials[period_type]
        fiscal_years = period_block.get("Fiscal Year", [])

        for block_name in FINANCIAL_BLOCKS:
            if block_name not in period_block:
                continue

            block_data = period_block[block_name]

            for metric_name, values_list in block_data.items():
                metric_id = mapping[block_name].get(metric_name)

                for idx, raw_value in enumerate(values_list):
                    value = valid_number(raw_value)
                    if value is None:
                        continue

                    fy = fiscal_years[idx]
                    year, month = extract_year_month(fy)
                    if not year:
                        continue

                    rows.append((
                        ticker,
                        period_type,
                        year,
                        month,
                        metric_id,
                        value
                    ))

    return rows


# ---------------------------------------------------------
# 4. Insert into fundamental_data
# ---------------------------------------------------------
def insert_fundamental_data(rows):
    if not rows:
        return

    sql = """
        INSERT INTO fundamental_data
            (ticker, period, year, month, fundamental_data_type_id, value)
        VALUES %s
        ON CONFLICT ON CONSTRAINT fundamental_data_unique DO NOTHING;
    """
    execute_values(cursor, sql, rows)
    conn.commit()


# ---------------------------------------------------------
# MAIN EXECUTION
# ---------------------------------------------------------
def main():

    # -------- PASS 1: scan all files for metric names ------
    print("Scanning metrics...")
    metrics = scan_all_metric_names()

    # -------- PASS 2: create all fundamental_data_type rows ------
    print("Creating metric type rows...")
    mapping = create_fundamental_data_type_rows(metrics)

    # -------- PASS 3: parse + insert all data ------
    print("Importing data...")
    batch = []

    for filename in os.listdir(DATA_FOLDER):
        if not filename.endswith(".json"):
            continue

        filepath = os.path.join(DATA_FOLDER, filename)
        rows = process_file(filepath, mapping)
        batch.extend(rows)

        if len(batch) > 50000:
            insert_fundamental_data(batch)
            batch = []

    insert_fundamental_data(batch)

    print("Completed successfully!")


if __name__ == "__main__":
    main()
