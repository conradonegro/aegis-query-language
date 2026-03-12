import os


def clean_sql_file():
    target_dir = "data/processed"
    os.makedirs(target_dir, exist_ok=True)

    src_file = "data/minidev/MINIDEV_postgresql/BIRD_dev.sql"
    dest_file = os.path.join(target_dir, "01_BIRD_dev.sql")  # Prefix with 01 so Docker executes it in order

    print(f"Cleaning BIRD-SQL: {src_file} -> {dest_file}")

    with open(src_file, 'rb') as f_in, open(dest_file, 'wb') as f_out:
        for line_no, raw_line in enumerate(f_in, 1):
            # Decode using replace, swap unicode replacement char for ascii '?'
            line = raw_line.decode('utf-8', errors='replace').replace('\ufffd', '')

            # Phase 1 Constraint: Between 2242 and 3900956 is purely RAW DATA. Do not mess with structure here.
            is_data = 2242 <= line_no <= 3900956

            if not is_data:
                # 1. Strip hardcoded OWNER constraints
                if "OWNER TO" in line and ("xiaolongli" in line or "johndoe" in line):
                    continue

                # 2. Schema Agnostic: make objects reside in the default container schema
                line = line.replace("CREATE TABLE public.", "CREATE TABLE ")
                line = line.replace("ALTER TABLE public.", "ALTER TABLE ")
                line = line.replace("ALTER TABLE ONLY public.", "ALTER TABLE ONLY ")
                line = line.replace("REFERENCES public.", "REFERENCES ")
                line = line.replace("COPY public.", "COPY ")

                # 3. Restore Default Search Path (pg_dump exports set it to '' which crashes schema-agnostic DDL)
                line = line.replace("search_path', '', false", "search_path', 'public', false")

            f_out.write(line.encode('utf-8'))

def clean_csv_metadata():
    # Fix the non-utf8 issues identified in Phase 1
    corrupt_files = [
        "data/minidev/MINIDEV/dev_databases/formula_1/database_description/qualifying.csv",
        "data/minidev/MINIDEV/dev_databases/european_football_2/database_description/Team_Attributes.csv",
        "data/minidev/MINIDEV/dev_databases/european_football_2/database_description/Player_Attributes.csv",
        "data/minidev/MINIDEV/dev_databases/student_club/database_description/Budget.csv"
    ]

    for csv_file in corrupt_files:
        if os.path.exists(csv_file):
            print(f"Cleaning CSV Encoding: {csv_file}")
            with open(csv_file, 'rb') as f:
                content = f.read().decode('utf-8', errors='replace').replace('\ufffd', '?')
            with open(csv_file, 'wb') as f:
                f.write(content.encode('utf-8'))

if __name__ == "__main__":
    clean_sql_file()
    clean_csv_metadata()
    print("Cleanup Complete!")
