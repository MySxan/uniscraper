import pandas as pd

df = pd.read_csv('../data/merged_universities.csv')

print(f"Total raw entries: {len(df)}")

# filter Unknown
df_unknown = df[df['country'] == 'Unknown'].copy()

print(f"Unknown Location: {len(df_unknown)}")

df_final = df_unknown[['name', 'country']].copy()

output_file = '../data/universities_unknown.csv'
df_final.to_csv(output_file, index=False)

print(f"\nSaved to {output_file}")
print(f"Include {len(df_final)} universities with unknown location")

# source statistics
unknown_full = df[df['country'] == 'Unknown']
source_counts = {}
for sources in unknown_full['sources']:
    for source in sources.split(', '):
        source_counts[source] = source_counts.get(source, 0) + 1

for source, count in sorted(source_counts.items()):
    print(f"- {source}: {count}")
