import kagglehub
import pandas as pd
from pathlib import Path
import sys


def main():
    """Main workflow: download dataset and generate 2025 CSV"""

    print("=" * 80)
    print("THE World University Rankings 2025 - CSV Generator")
    print("=" * 80)
    print()

    try:
        # Step 1: Download dataset from Kaggle
        print("1. Downloading dataset from Kaggle...")
        dataset_path = kagglehub.dataset_download(
            "raymondtoo/the-world-university-rankings-2016-2024"
        )
        print(f"   ✓ Downloaded to: {dataset_path}")
        print()

        # Step 2: Load the CSV
        print("2. Loading data...")
        csv_file = Path(dataset_path) / "THE World University Rankings 2016-2025.csv"
        df = pd.read_csv(csv_file)
        print(f"   ✓ Loaded {len(df):,} records from CSV")
        print()

        # Step 3: Process 2025 data
        print("3. Processing 2025 data...")
        df_2025 = df[df["Year"] == 2025].copy()
        print(f"   ✓ Found {len(df_2025):,} universities in 2025")
        print()

        # Step 4: Select and save columns
        print("4. Saving to CSV...")
        output_file = Path(__file__).parent.parent / "output" / "THE_2025_Rankings.csv"
        df_output = df_2025[["Rank", "Name", "Country"]].sort_values("Rank")
        df_output.to_csv(output_file, index=False)

        file_size_kb = output_file.stat().st_size / 1024
        print(f"   ✓ Saved to: {output_file}")
        print()

        # Step 5: Display summary
        print("5. Summary:")
        print(f"   - File: {output_file.name}")
        print(f"   - Records: {len(df_output):,}")
        print(f"   - Columns: {list(df_output.columns)}")
        print(f"   - Size: {file_size_kb:.1f} KB")
        print()

        # Step 6: Show first 10 records
        print("6. First 10 records:")
        print()
        print(df_output.head(10).to_string(index=False))
        print()

        print("=" * 80)
        print("✓ Complete! CSV file is ready.")
        print("=" * 80)

    except Exception as e:
        print(f"❌ Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
