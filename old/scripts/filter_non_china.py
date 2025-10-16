import pandas as pd

df = pd.read_csv('../data/merged_universities.csv')

print(f"Total raw entries: {len(df)}")

china_regions = ['China', 'Hong Kong', 'Macau', 'Taiwan']

# filter out Chinese regions
df_non_china = df[~df['country'].isin(china_regions)].copy()

print(f"Chinese regions: {len(df) - len(df_non_china)}")
print(f"Non-Chinese regions: {len(df_non_china)}")

df_final = df_non_china[['name', 'country']].copy()
output_file = '../data/universities_non_china.csv'
df_final.to_csv(output_file, index=False)

print(f"\nSaved to {output_file}")
print(f"Include {len(df_final)} universities from non-Chinese regions")
