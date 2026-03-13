import os


def scan_bird_sql_files(base_dir: str) -> None:  # noqa: C901
    print("=== BIRD-SQL PHASE 1: SCRIPT REVIEW ===\n")

    issues = []
    table_counts = {}

    # Files/folders to review:
    # 1. dev_databases/ (folders contain csv files, check for encoding or bad
    #    syntax in definitions)
    # 2. dev_tables.json
    # 3. mini_dev_postgresql.json
    # 4. mini_dev_postgresql_gold.sql
    # 5. BIRD_dev.sql (large file with line constraints)

    dev_databases_dir = os.path.join(base_dir, 'MINIDEV', 'dev_databases')
    dev_tables_json = os.path.join(base_dir, 'MINIDEV', 'dev_tables.json')
    mini_dev_json = os.path.join(base_dir, 'MINIDEV', 'mini_dev_postgresql.json')
    gold_sql = os.path.join(base_dir, 'MINIDEV', 'mini_dev_postgresql_gold.sql')
    bird_dev_sql = os.path.join(
        base_dir, 'MINIDEV_postgresql', 'BIRD_dev.sql'
    )

    # 1. Scan dev_databases (csv files)
    if os.path.exists(dev_databases_dir):
        print("Scanning dev_databases csv files...")
        for root, _dirs, files in os.walk(dev_databases_dir):
            for file in files:
                if file.endswith('.sqlite'):
                    continue
                if file.endswith('.csv'):
                    filepath = os.path.join(root, file)
                    with open(filepath, 'rb') as f:
                        try:
                            f.read().decode('utf-8')
                        except UnicodeDecodeError as e:
                            issues.append(
                                f"Encoding Issue (CSV): {filepath}"
                                f" contains non-UTF8 characters. Error: {e}"
                            )

    # 2. Scan dev_tables.json and mini_dev_postgresql.json
    for json_file in [dev_tables_json, mini_dev_json]:
        if os.path.exists(json_file):
            print(f"Scanning {os.path.basename(json_file)}...")
            with open(json_file, 'rb') as f:
                try:
                    f.read().decode('utf-8')
                except UnicodeDecodeError as e:
                    issues.append(
                        f"Encoding Issue (JSON): {json_file}"
                        f" contains non-UTF8 characters. Error: {e}"
                    )

    # 3. Scan mini_dev_postgresql_gold.sql
    if os.path.exists(gold_sql):
        print(f"Scanning {os.path.basename(gold_sql)}...")
        with open(gold_sql, 'rb') as f:
            for line_no, raw_line in enumerate(f, 1):
                try:
                    line = raw_line.decode('utf-8')
                except UnicodeDecodeError:
                    issues.append(
                        f"Encoding Issue (Gold SQL): {gold_sql} Line {line_no}"
                        f" contains non-UTF8 characters."
                    )
                    line = raw_line.decode('utf-8', errors='replace')
                if "xiaolongli" in line or "johndoe" in line:
                    issues.append(
                        f"Hardcoded Owner (Gold SQL): {gold_sql} Line"
                        f" {line_no} refers to {line.strip()}"
                    )

    # 4. Scan BIRD_dev.sql via explicit line constraints
    if os.path.exists(bird_dev_sql):
        print(f"Scanning {os.path.basename(bird_dev_sql)}...")
        current_table = None

        with open(bird_dev_sql, 'rb') as f:
            for line_no, raw_line in enumerate(f, 1):
                try:
                    line = raw_line.decode('utf-8')
                except UnicodeDecodeError as e:
                    issues.append(
                        f"Encoding Issue (BIRD SQL): {os.path.basename(bird_dev_sql)}"
                        f" Line {line_no} contains non-UTF8 characters: {str(e)}"
                    )
                    line = raw_line.decode('utf-8', errors='replace')

                # Check for explicit owner references (schema agnostic) in the
                # DDL ranges. Add check for non-standard syntax? Hardcoded
                # schemas. Exclude data lines which just have it in comments if
                # we want to be strict, but we'll scan anyway.
                if line_no < 2242 or line_no > 3900956:
                    if "xiaolongli" in line or "johndoe" in line or "OWNER TO" in line:
                        if len(issues) < 50: # Cap output flood
                            issues.append(
                                f"Hardcoded Owner (BIRD SQL): Line"
                                f" {line_no} -> {line.strip()}"
                            )

                # We are scanning for "COPY public.table_name" to track row
                # insertions; operates between 2242 to 3900956 per user
                # constraints
                if 2242 <= line_no <= 3900956:
                    if line.startswith("COPY public.") and "FROM stdin;" in line:
                        parts = line.split(" ")
                        if len(parts) > 1 and parts[1].startswith("public."):
                            current_table = parts[1].replace("public.", "")
                            if current_table not in table_counts:
                                table_counts[current_table] = 0
                        continue

                    # If we are inside a COPY block, count the rows
                    if (
                        current_table and line.strip()
                        and not line.startswith("--")
                        and not line.startswith("\\.")
                        and not line.startswith("COPY ")
                    ):
                        table_counts[current_table] += 1

                    # Reset current_table when COPY block ends
                    if line.startswith("\\."):
                        current_table = None

    print("\n--- ISSUES DETECTED ---")
    if len(issues) == 0:
        print("No structural or encoding issues found.")
    else:
        for _, iss in enumerate(issues[:50]):
            print(iss)
        if len(issues) > 50:
            print(f"... and {len(issues) - 50} more issues suppressed.")

    print("\n--- BIRD_dev.sql ROW COUNTS ---")
    total_tables = len(table_counts)
    total_rows = sum(table_counts.values())
    print(f"Total Tables Detected in COPY blocks: {total_tables}")
    print(f"Total Rows Detected across all blocks: {total_rows:,}")

    # Just printing the top 20 and bottom 5 for brevity since there might be
    # many tables
    sorted_tables = sorted(table_counts.items())
    if len(sorted_tables) > 25:
        print("\n[Sample Tables]")
        for table, count in sorted_tables[:10]:
            print(f" {table}: {count:,} rows")
        print(" ...")
        for table, count in sorted_tables[-5:]:
            print(f" {table}: {count:,} rows")
    else:
        for table, count in sorted_tables:
            print(f" {table}: {count:,} rows")

if __name__ == "__main__":
    scan_bird_sql_files("data/minidev")
