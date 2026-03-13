import sys


def scan_bird_sql(filepath: str) -> None:  # noqa: C901
    """
    Phase 1: Script Review Requirements
    - Identify encoding issues (Non-UTF8)
    - Hardcoded schema/owner names (e.g. johndoe / xiaolongli)
    - Table count and total row count per table
    """
    table_counts = {}
    current_table = None
    issues = []

    print(f"Starting scan of {filepath}...")

    try:
        with open(filepath, 'rb') as f:
            for line_no, raw_line in enumerate(f, 1):
                try:
                    # Attempt strict UTF-8 decoding to find encoding issues
                    line = raw_line.decode('utf-8')
                except UnicodeDecodeError as e:
                    issues.append(
                        f"Line {line_no}: Non-UTF8 Character Detected - {str(e)}"
                    )
                    line = raw_line.decode('utf-8', errors='replace')

                # We are scanning for "COPY public.table_name" to track row
                # insertions. This operates between 2242 to 3900956 per the
                # user constraints
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

                # Check for explicit owner references (schema agnostic) in the
                # DDL ranges
                if line_no < 2242 or line_no > 3900956:
                    if (
                        "xiaolongli" in line
                        or "johndoe" in line
                        or "OWNER TO" in line
                    ):
                        if len(issues) < 20: # Cap output flood
                            issues.append(
                                f"Line {line_no}: Hardcoded Owner"
                                f" discovered -> {line.strip()}"
                            )

    except Exception as e:
        print(f"Failed to scan: {e}")
        return

    print("\n=== BIRD-SQL SCAN RESULTS ===")
    print(f"Total Tables Detected: {len(table_counts)}")
    print("\n--- Row Counts per Table ---")
    for table, count in sorted(table_counts.items()):
        print(f"{table}: {count:,} rows")

    print(f"\n--- Issues Detected ({len(issues)} logged) ---")
    for iss in issues[:20]:
        print(iss)
    if len(issues) > 20:
        print(f"... and {len(issues) - 20} more issues.")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python scan_bird_sql.py <path_to_bird_dev>")
        sys.exit(1)
    scan_bird_sql(sys.argv[1])
