# CSV Data Cleaning and Consolidation

This Python project script multiple CSV files, clean and normalizes the data based in a reference CSV files, and create a master dataset along with filterd subsets for tyres and rims. It also generate provider-specific CSV files. The script uses pandas for data handling and SQLite for temporary storage of the data

Collaboration with (trucnguyenlethanh)[https://github.com/trucnguyenlethanh]

## Requirements
- pandas library
```bash
pip install pandas
```
- sqlite3 (Python standard library)

## Explaination
The script is designed to facilitate the cleaning, normalization, and consolidation of heterogeneous CSV datasets into a unified, analyzable format.
    1.  Environment Setup
Initially, the script creates a ‘result/‘ directory to hold processed data. It then deletes any existing ‘staging.db’ database to ensure that subsequent processing starts fresh, avoiding any interference from previous runs.
    2.  Reference Format Initialization
The script uses a reference CSV file, 09.csv, as a structural template to standardize all subsequent datasets. It only reads the header row of the reference file to get the column names. These column names are normalized by removing whitespace, converting to lowercase, and replacing spaces with underscores, ensuring consistency across all imported datasets.
    3.  Data Ingestion and Cleaning
CSV files in resource_files/ are processed in 50,000-row chunks to optimize memory. Each dataset is standardized to match the reference column format. A key data quality check involves validating the European Article Number (EAN), retaining only 13-digit numeric values. Rows failing this check are discarded to ensure data integrity. The source filename is also added as metadata for provenance tracking.
    4.  Construction of the Master Dataset
Once all individual files are ingested into the SQLite staging database, the data are extracted into a consolidated DataFrame. Missing product_id values are filled with a placeholder (‘N/A’) to maintain structural consistency. The dataset is then deduplicated using a grouping operation based on the EAN and product_id attributes. A provider field is added, derived from the originating CSV file, which enables subsequent categorization and analysis. The final master dataset is exported as master_data.csv.
    5.  Domain-Specific Filtering: Tyres and Rims
The script creates two specialized datasets, tyres and rims to aid domain-specific analysis. Tyres are recognized by keywords like “REIFEN” in the article_type or prefixes such as TYRE/REIFEN in product_id. Rims are identified with keywords like “FEL” or prefixes like RIM/FELGE. These filtered datasets are saved as tyres.csv and rims.csv, providing focused data for industry-related uses.
    6.  Provider-Level Data Consolidation
Finally, the script finds providers with records in both the tyres and rims datasets. For each provider, the relevant data is merged into one CSV file named {provider}_tyres_rims.csv. This method allows stakeholders to see a complete overview of products tied to specific providers while keeping the detail needed for individual analysis.
