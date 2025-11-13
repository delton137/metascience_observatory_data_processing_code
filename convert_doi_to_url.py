import pandas as pd

# Read the CSV file
df = pd.read_csv('replications_database_2025_11_01.csv')

# Add http://doi.org/ prefix to the DOI columns
df['original_doi'] = 'http://doi.org/' + df['original_doi'].astype(str)
df['replication_doi'] = 'http://doi.org/' + df['replication_doi'].astype(str)

# Rename the columns
df = df.rename(columns={
    'original_doi': 'original_url',
    'replication_doi': 'replication_url'
})

# Save the modified dataframe back to CSV
df.to_csv('replications_database_2025_11_01.csv', index=False)

print("✓ Successfully updated CSV file:")
print(f"  - Added 'http://doi.org/' prefix to DOI columns")
print(f"  - Renamed 'original_doi' to 'original_url'")
print(f"  - Renamed 'replication_doi' to 'replication_url'")
print(f"  - Total rows processed: {len(df)}")
