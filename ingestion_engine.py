"""
Ingestion Engine for Replications Database

This script processes spreadsheets containing replication experiment data
and adds them to the master replications database.

Usage:
    python ingestion_engine.py <input_csv_file> <master_database_csv>
    python ingestion_engine.py --skip-api-calls <input_csv_file> <master_database_csv>
"""

import pandas as pd
import argparse
import time
from datetime import datetime
from fetch_metadata_from_doi import fetch_metadata_from_doi
from fetch_metadata_from_title import fetch_metadata_from_title
from generate_citation_html_for_website import generate_citation_html_for_website

def extract_doi_from_url(url):
    """Extract DOI from URL like 'http://doi.org/10.1234/xyz'"""
    if not isinstance(url, str) or not url.strip():
        return None
    url = url.strip()
    if url.startswith("http://doi.org/"):
        return url.replace("http://doi.org/", "")
    elif url.startswith("https://doi.org/"):
        return url.replace("https://doi.org/", "")
    return None

def normalize_doi(doi):
    """
    Normalize a DOI by removing any URL prefix.
    Handles cases where DOI might already be a full URL.
    Returns just the DOI part (e.g., '10.1234/xyz')
    """
    if not isinstance(doi, str) or not doi.strip():
        return None
    doi = doi.strip()
    # Strip common URL prefixes
    if doi.startswith("http://doi.org/"):
        doi = doi.replace("http://doi.org/", "")
    elif doi.startswith("https://doi.org/"):
        doi = doi.replace("https://doi.org/", "")
    elif doi.startswith("http://dx.doi.org/"):
        doi = doi.replace("http://dx.doi.org/", "")
    elif doi.startswith("https://dx.doi.org/"):
        doi = doi.replace("https://dx.doi.org/", "")
    return doi if doi else None

def is_empty(value):
    """Check if value is empty/missing"""
    return pd.isna(value) or value == "" or value == "NaN" or (isinstance(value, str) and not value.strip())

def needs_enrichment(row, prefix):
    """Check if any key metadata fields are missing or abbreviated"""
    fields_to_check = ['authors', 'title', 'journal', 'volume', 'issue', 'pages', 'year']

    # Check if any field is missing
    for field in fields_to_check:
        col_name = f"{prefix}_{field}"
        if col_name in row.index and is_empty(row[col_name]):
            return True

    # Check if authors contains abbreviated names (single letter first names like "J.")
    authors = row.get(f"{prefix}_authors")
    if isinstance(authors, str) and authors.strip():
        # Check for pattern like "J. " or "M. " (abbreviated first names)
        if any(f" {c}. " in authors or authors.startswith(f"{c}. ") for c in "ABCDEFGHIJKLMNOPQRSTUVWXYZ"):
            return True

    # Check if journal is highly abbreviated (less than 10 chars, likely abbreviated)
    journal = row.get(f"{prefix}_journal")
    if isinstance(journal, str) and len(journal.strip()) < 10 and "." in journal:
        return True

    return False

def enrich_from_metadata(row, prefix, metadata):
    """Fill row with metadata from API calls"""
    if not metadata:
        return row

    field_mapping = {
        'authors': f'{prefix}_authors',
        'title': f'{prefix}_title',
        'journal': f'{prefix}_journal',
        'volume': f'{prefix}_volume',
        'issue': f'{prefix}_issue',
        'pages': f'{prefix}_pages',
        'year': f'{prefix}_year',
    }

    for meta_key, col_name in field_mapping.items():
        if col_name in row.index:
            # Only fill if current value is empty
            if is_empty(row[col_name]) and metadata.get(meta_key):
                row[col_name] = metadata[meta_key]

    return row

def sanity_check_metadata(row, prefix, metadata):
    """
    Check if fetched metadata matches existing data.
    Returns True if metadata is likely correct, False otherwise.
    Only checks the year field.
    """
    if not metadata:
        return False

    # Check year field only
    col_name = f"{prefix}_year"
    if col_name in row.index and not is_empty(row[col_name]):
        existing_value = str(row[col_name]).strip()
        fetched_value = str(metadata.get('year', "")).strip()

        print(fetched_value, existing_value)

        if fetched_value:
            # Handle float values like "2020.0" vs "2020"
            if existing_value.replace(".0", "") == fetched_value.replace(".0", ""):
                return True
            else:
                return False
    
    # If no year data to verify against, assume correct
    return True

def process_row(row, row_idx, total_rows):
    """Process a single row to enrich metadata"""
    print(f"\nProcessing row {row_idx + 1}/{total_rows}...")

    # ===== PROCESS ORIGINAL STUDY =====
    original_url = row.get('original_url')
    original_doi = extract_doi_from_url(original_url)

    if original_doi and needs_enrichment(row, 'original'):
        print(f"  Fetching metadata for original DOI: {original_doi}")
        metadata = fetch_metadata_from_doi(original_doi)
        row = enrich_from_metadata(row, 'original', metadata)
        time.sleep(0.3)  # Rate limiting

    # If no DOI URL but title exists, try to fetch DOI from title
    elif is_empty(original_url) and not is_empty(row.get('original_title')):
        print(f"  No original_url found, searching by title: {row.get('original_title')}...")
        metadata = fetch_metadata_from_title(row.get('original_title'))

        if metadata and metadata.get('doi'):
            # Sanity check the DOI
            if sanity_check_metadata(row, 'original', metadata):
                # Normalize DOI to handle cases where it's already a full URL
                normalized_doi = normalize_doi(metadata['doi'])
                if normalized_doi:
                    print(f"  ✓ Found and verified DOI: {normalized_doi}")
                    row['original_url'] = f"http://doi.org/{normalized_doi}"
                    row = enrich_from_metadata(row, 'original', metadata)
                else:
                    print(f"  ✗ Could not normalize DOI: {metadata['doi']}")
            else:
                print(f"  ✗ DOI failed sanity check, not using: {metadata['doi']}")
        else:
            print(f"  ✗ Could not find DOI from title")

        time.sleep(0.3)  # Rate limiting

    # ===== PROCESS REPLICATION STUDY =====
    replication_url = row.get('replication_url')
    replication_doi = extract_doi_from_url(replication_url)

    if replication_doi and needs_enrichment(row, 'replication'):
        print(f"  Fetching metadata for replication DOI: {replication_doi}")
        metadata = fetch_metadata_from_doi(replication_doi)
        row = enrich_from_metadata(row, 'replication', metadata)
        time.sleep(0.3)  # Rate limiting

    # If no DOI URL but title exists, try to fetch DOI from title
    elif is_empty(replication_url) and not is_empty(row.get('replication_title')):
        print(f"  No replication_url found, searching by title: {row.get('replication_title')[:50]}...")
        metadata = fetch_metadata_from_title(row.get('replication_title'))

        if metadata and metadata.get('doi'):
            # Sanity check the DOI
            if sanity_check_metadata(row, 'replication', metadata):
                # Normalize DOI to handle cases where it's already a full URL
                normalized_doi = normalize_doi(metadata['doi'])
                if normalized_doi:
                    print(f"  ✓ Found and verified DOI: {normalized_doi}")
                    row['replication_url'] = f"http://doi.org/{normalized_doi}"
                    row = enrich_from_metadata(row, 'replication', metadata)
                else:
                    print(f"  ✗ Could not normalize DOI: {metadata['doi']}")
            else:
                print(f"  ✗ DOI failed sanity check, not using: {metadata['doi']}")
        else:
            print(f"  ✗ Could not find DOI from title")

        time.sleep(0.3)  # Rate limiting

    return row

def generate_citations(df):
    """Generate HTML citations for display on website"""
    print("\nGenerating HTML citations...")

    # Extract DOI from URL for citation generation
    def get_doi_for_citation(url):
        doi = extract_doi_from_url(url)
        return doi if doi else ""

    df["replication_citation_html"] = df.apply(
        lambda row: generate_citation_html_for_website(
            row.get("replication_authors"),
            row.get("replication_journal"),
            row.get("replication_year"),
            get_doi_for_citation(row.get("replication_url")),
        ),
        axis=1
    )

    df["original_citation_html"] = df.apply(
        lambda row: generate_citation_html_for_website(
            row.get("original_authors"),
            row.get("original_journal"),
            row.get("original_year"),
            get_doi_for_citation(row.get("original_url")),
        ),
        axis=1
    )

    return df

def filter_columns(df, data_dict_path='data_dictionary.csv'):
    """Keep only columns that appear in data_dictionary.csv, preserving order from data dictionary"""
    print("\nFiltering columns based on data_dictionary.csv...")

    data_dict = pd.read_csv(data_dict_path)
    valid_columns = data_dict['column_name'].tolist()

    # Keep only columns that exist in both the dataframe and the valid columns list
    # Order them according to the order in data_dictionary.csv
    columns_to_keep = [col for col in valid_columns if col in df.columns]

    print(f"  Keeping {len(columns_to_keep)} valid columns out of {len(df.columns)} total")
    print(f"  Columns ordered according to data_dictionary.csv")

    return df[columns_to_keep]

def normalize_discipline_column(df):
    """Convert discipline column values to lowercase"""
    if 'discipline' in df.columns:
        print("\nNormalizing discipline column (converting to lowercase)...")
        df['discipline'] = df['discipline'].apply(
            lambda x: x.lower() if pd.notna(x) and isinstance(x, str) else x
        )
        print(f"  ✓ Converted discipline values to lowercase")
    return df

def reorder_columns(df, data_dict_path='data_dictionary.csv'):
    """Reorder columns according to the order in data_dictionary.csv"""
    data_dict = pd.read_csv(data_dict_path)
    valid_columns = data_dict['column_name'].tolist()
    
    # Get columns that exist in both the dataframe and the data dictionary
    # Order them according to the order in data_dictionary.csv
    columns_in_order = [col for col in valid_columns if col in df.columns]
    
    # Add any columns that exist in df but not in data dictionary (shouldn't happen after filtering, but just in case)
    remaining_columns = [col for col in df.columns if col not in columns_in_order]
    
    # Combine: ordered columns first, then any remaining columns
    final_column_order = columns_in_order + remaining_columns
    
    return df[final_column_order]

def check_duplicate(row, master_df):
    """
    Check if row is duplicate based on original_url, replication_url, and description.
    Returns True if duplicate found.
    """
    if master_df.empty:
        return False

    # Get values to check (handle different column names)
    original_check = row.get('original_url')
    replication_check = row.get('replication_url')
    description_check = row.get('description')

    # Check for exact match on all three fields
    matches = master_df[
        (master_df['original_url'] == original_check) &
        (master_df['replication_url'] == replication_check) &
        (master_df['description'] == description_check)
    ]

    return len(matches) > 0

def ingest_data(input_csv, master_csv, skip_api_calls=False):
    """Main ingestion function"""
    print(f"\n{'='*60}")
    print(f"REPLICATIONS DATABASE INGESTION ENGINE")
    print(f"{'='*60}")
    if skip_api_calls:
        print("  [Skipping API calls - metadata enrichment disabled]")
    print(f"{'='*60}")

    # Load input data
    print(f"\nLoading input file: {input_csv}")
    input_df = pd.read_csv(input_csv)
    print(f"  Loaded {len(input_df)} rows")

    # Load master database
    print(f"\nLoading master database: {master_csv}")
    try:
        master_df = pd.read_csv(master_csv)
        print(f"  Loaded {len(master_df)} existing rows")
    except FileNotFoundError:
        print(f"  Master database not found, will create new one")
        master_df = pd.DataFrame()

    # Process each row (skip API calls if flag is set)
    if skip_api_calls:
        print(f"\n{'='*60}")
        print(f"STEP 1: SKIPPING METADATA ENRICHMENT (--skip-api-calls flag set)")
        print(f"{'='*60}")
        processed_df = input_df.copy()
    else:
        print(f"\n{'='*60}")
        print(f"STEP 1: ENRICHING METADATA")
        print(f"{'='*60}")

        processed_rows = []
        for idx, row in input_df.iterrows():
            processed_row = process_row(row, idx, len(input_df))
            processed_rows.append(processed_row)

        processed_df = pd.DataFrame(processed_rows)

    # Generate citations
    print(f"\n{'='*60}")
    print(f"STEP 2: GENERATING CITATIONS HTML")
    print(f"{'='*60}")
    processed_df = generate_citations(processed_df)

    # Filter columns
    print(f"\n{'='*60}")
    print(f"STEP 3: FILTERING COLUMNS")
    print(f"{'='*60}")
    processed_df = filter_columns(processed_df)

    # Normalize discipline column
    processed_df = normalize_discipline_column(processed_df)

    # Check for duplicates and append
    print(f"\n{'='*60}")
    print(f"STEP 4: CHECKING DUPLICATES AND APPENDING")
    print(f"{'='*60}")

    rows_to_append = []
    duplicates_found = 0

    for idx, row in processed_df.iterrows():
        if check_duplicate(row, master_df):
            print(f"\n⚠️  WARNING: Row {idx + 1} is a duplicate (matching original_url, replication_url, and description)")
            print(f"    Original: {row.get('original_url')}")
            print(f"    Replication: {row.get('replication_url')}")
            print(f"    Description: {row.get('description', '')[:80]}...")
            duplicates_found += 1
        else:
            rows_to_append.append(row)

    print(f"\n  Found {duplicates_found} duplicates (skipped)")
    print(f"  Adding {len(rows_to_append)} new rows to master database")

    # Append new rows to master
    if rows_to_append:
        new_rows_df = pd.DataFrame(rows_to_append)
        updated_master_df = pd.concat([master_df, new_rows_df], ignore_index=True)
    else:
        updated_master_df = master_df

    # Reorder columns according to data_dictionary.csv
    updated_master_df = reorder_columns(updated_master_df)

    # Save with timestamp
    print(f"\n{'='*60}")
    print(f"STEP 5: SAVING UPDATED DATABASE")
    print(f"{'='*60}")

    timestamp = datetime.now().strftime("%Y_%m_%d_%H%M%S")
    output_filename = f"replications_database_{timestamp}.csv"
    updated_master_df.to_csv(output_filename, index=False)
    print(f"\n✓ Saved updated database to: {output_filename}")
    print(f"  Total rows in database: {len(updated_master_df)}")

    # Update version history
    print(f"\nUpdating version_history.txt...")
    with open('version_history.txt', 'a') as f:
        f.write(f"{output_filename}\n")
    print(f"✓ Added to version_history.txt")

    print(f"\n{'='*60}")
    print(f"INGESTION COMPLETE!")
    print(f"{'='*60}")
    print(f"Summary:")
    print(f"  - Input rows: {len(input_df)}")
    print(f"  - Duplicates skipped: {duplicates_found}")
    print(f"  - New rows added: {len(rows_to_append)}")
    print(f"  - Total rows in database: {len(updated_master_df)}")
    print(f"  - Output file: {output_filename}")
    print()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Ingestion Engine for Replications Database",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python ingestion_engine.py psych_file_drawer_data_to_ingest.csv replications_database_2025_11_01.csv
  python ingestion_engine.py --skip-api-calls psych_file_drawer_data_to_ingest.csv replications_database_2025_11_01.csv
        """
    )
    parser.add_argument('input_csv', help='Input CSV file to ingest')
    parser.add_argument('master_csv', help='Master database CSV file')
    parser.add_argument('--skip-api-calls', action='store_true',
                       help='Skip metadata enrichment API calls (faster but no metadata updates)')
    
    args = parser.parse_args()

    ingest_data(args.input_csv, args.master_csv, skip_api_calls=args.skip_api_calls)
