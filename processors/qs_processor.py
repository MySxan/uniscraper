import pandas as pd
from pathlib import Path
import sys


def main():
    """Main workflow: read Excel and generate CSV"""

    print("=" * 80)
    print("QS 2026 World University Rankings - CSV Generator")
    print("=" * 80)
    print()

    try:
        # Step 1: Load the Excel file
        print("1. Loading Excel file...")
        excel_file = (
            Path(__file__).parent.parent
            / "raw"
            / "2026 QS World University Rankings 1.2 (For qs.com).xlsx"
        )

        if not excel_file.exists():
            print(f"   ❌ File not found: {excel_file}")
            sys.exit(1)

        # Skip first 2 rows (headers/metadata)
        df = pd.read_excel(excel_file, sheet_name=0, skiprows=2)
        print(f"   ✓ Loaded {len(df):,} records")
        print()

        # Step 2: Select required columns
        print("2. Processing data...")
        # Rename columns for clarity: Country/Territory is the country, Region is the region, Status is Public/Private
        df_output = df[["Rank", "Name", "Region", "Status"]].copy()

        # Remove any rows with missing critical data
        df_output = df_output.dropna(subset=["Rank", "Name"])

        print(f"   ✓ Selected columns: Rank, Name, Region, Status")
        print(f"   ✓ Records after cleaning: {len(df_output):,}")
        print()

        # Step 3: Save to CSV
        print("3. Saving to CSV...")
        output_file = Path(__file__).parent.parent / "output" / "QS_2026_Rankings.csv"
        df_output.to_csv(output_file, index=False)

        file_size_kb = output_file.stat().st_size / 1024
        print(f"   ✓ Saved to: {output_file}")
        print()

        # Step 4: Display summary
        print("4. Summary:")
        print(f"   - File: {output_file.name}")
        print(f"   - Records: {len(df_output):,}")
        print(f"   - Columns: {list(df_output.columns)}")
        print(f"   - Size: {file_size_kb:.1f} KB")
        print()

        # Step 5: Show first 10 records
        print("5. First 10 records:")
        print()
        print(df_output.head(10).to_string(index=False))
        print()

        print("=" * 80)
        print("✓ Complete! CSV file is ready.")
        print("=" * 80)

    except Exception as e:
        print(f"❌ Error: {e}", file=sys.stderr)
        import traceback

        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
