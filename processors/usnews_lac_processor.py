import pandas as pd
import re
from pathlib import Path
import sys


def parse_usnews_lac_txt(filepath):
    """Parse US News LAC txt file and extract rank, name, region"""
    
    records = []
    
    with open(filepath, 'r', encoding='utf-8') as f:
        lines = f.readlines()
    
    i = 0
    while i < len(lines):
        line = lines[i].strip()
        
        # Look for rank pattern like "#1", "#2", "#5 (tie)" etc
        if line.startswith('#') and 'in' in lines[i+1].strip() if i+1 < len(lines) else False:
            # Extract rank number
            rank_text = line.replace('#', '').split()[0]
            try:
                rank = int(rank_text)
            except:
                i += 1
                continue
            
            # Skip "in" line and category line
            i += 2  # skip "#X" line and "in" line
            i += 1  # skip category line
            
            # Next should be college name or tuition line
            # Skip through data fields until we find the name and location
            name = None
            location = None
            
            # Go back and find name (name appears right before the rank)
            j = i - 4
            while j >= 0:
                prev_line = lines[j].strip()
                if prev_line and not prev_line.startswith('#') and not prev_line == 'in' and ',' in prev_line:
                    # This is likely the location line
                    location = prev_line
                    if j > 0:
                        name = lines[j-1].strip()
                    break
                j -= 1
            
            if name and location:
                # Extract state/region from location (e.g., "Williamstown, MA" -> "MA")
                try:
                    region = location.split(',')[-1].strip()
                except:
                    region = location
                
                records.append({
                    'Rank': rank,
                    'Name': name,
                    'Region': region
                })
        
        i += 1
    
    return pd.DataFrame(records)


def main():
    """Main workflow"""
    
    print("=" * 80)
    print("US News LAC Rankings - CSV Generator")
    print("=" * 80)
    print()
    
    try:
        # Step 1: Load the text file
        print("1. Loading text file...")
        txt_file = Path(__file__).parent.parent / 'raw' / 'USnews_lac.txt'
        
        if not txt_file.exists():
            print(f"   ❌ File not found: {txt_file}")
            sys.exit(1)
        
        df = parse_usnews_lac_txt(txt_file)
        print(f"   ✓ Parsed {len(df):,} records")
        print()
        
        # Step 2: Save to CSV
        print("2. Saving to CSV...")
        output_file = Path(__file__).parent.parent / 'output' / 'USnews_LAC_Rankings.csv'
        df.to_csv(output_file, index=False)
        
        file_size_kb = output_file.stat().st_size / 1024
        print(f"   ✓ Saved to: {output_file}")
        print()
        
        # Step 3: Display summary
        print("3. Summary:")
        print(f"   - File: {output_file.name}")
        print(f"   - Records: {len(df):,}")
        print(f"   - Columns: {list(df.columns)}")
        print(f"   - Size: {file_size_kb:.1f} KB")
        print()
        
        # Step 4: Show first 10 records
        print("4. First 10 records:")
        print()
        print(df.head(10).to_string(index=False))
        print()
        
        print("=" * 80)
        print("✓ Complete! CSV file is ready.")
        print("=" * 80)
        
    except Exception as e:
        print(f"❌ Error: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
