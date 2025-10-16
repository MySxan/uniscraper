import pandas as pd
from rapidfuzz import fuzz, process
import unicodedata
import re

def normalize_name(name):
    name = name.lower().strip()
    name = unicodedata.normalize('NFKD', name).encode('ASCII', 'ignore').decode('utf-8')
    name = re.sub(r'[^a-z0-9\s]', '', name)
    name = re.sub(r'\s+', ' ', name).strip()
    return name

# set
files = {
    # "../data/uniranks_cn.csv": "uniRank-China",
    "../data/uniranks_world.csv": "uniRank-World",
    # "../data/THE.csv": "THE",
    # "../data/shanghai_arwu.csv": "ARWU"
}

similarity_threshold = 90  # set similarity threshold

# read and label sources
dfs = []
for file, source in files.items():
    df = pd.read_csv(file)
    
    if "location" in df.columns:
        df = df.rename(columns={"location": "country"})
    elif "country" not in df.columns:
        df["country"] = "Unknown"

    df = df[["name", "country"]]
    df["name_normalized"] = df["name"].apply(normalize_name)
    df["source"] = source
    dfs.append(df)
    print(f"Loaded {file}: {len(df)} entries")

# merge data
all_unis = pd.concat(dfs, ignore_index=True)
print(f"Total raw entries: {len(all_unis)}")

# deduplicate
print("\nBegin deduplication...")
unique_records = []
seen_names = []  
name_mapping = {}  

for idx, row in all_unis.iterrows():
    name_original = row["name"]  
    name_normalized = row["name_normalized"] 
    country = row["country"]
    source = row["source"]
    
    matched_name = None
    for existing_normalized in seen_names:
        if fuzz.ratio(name_normalized, existing_normalized) >= similarity_threshold:
            matched_name = existing_normalized
            break
    
    if matched_name:
        for record in unique_records:
            if record["name_normalized"] == matched_name:
                record["sources"].add(source)
                if country != "Unknown" and record["country"] == "Unknown":
                    record["country"] = country
                elif country != "Unknown" and record["country"] != "Unknown" and country != record["country"]:
                    pass
                break
    else:
        seen_names.append(name_normalized)
        unique_records.append({
            "name": name_original,  
            "name_normalized": name_normalized,
            "country": country,
            "sources": {source}
        })

print(f"Entries after deduplication: {len(unique_records)} ")

# save result
final_records = []
for record in unique_records:
    final_records.append({
        "name": record["name"],
        "country": record["country"],
        "sources": ", ".join(sorted(record["sources"]))
    })

merged_df = pd.DataFrame(final_records)
merged_df = merged_df.sort_values(by=["country", "name"]).reset_index(drop=True)
merged_df.to_csv("../data/merged_universities.csv", index=False)

print(f"\nSaved to ../data/merged_universities.csv")
print(f"Total unique universities: {len(merged_df)}")
print(f"Universities with multiple sources: {len(merged_df[merged_df['sources'].str.contains(',', na=False)])}")
