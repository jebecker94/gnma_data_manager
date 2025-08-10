# GNMA Data Manager
A tool to make it easier to use the publicly available Ginnie Mae (GNMA) data.

## Functionality
This program takes raw data from Ginnie Mae's public disclosure data files, cleans it, and combines it into a large research-ready dataset.

### Parsing GNMA PDF Schemas
The scripts include tools for parsing the PDF schemas released by GNMA for handling the fixed-width and delimited files

## How To Use
1. Download data from the GNMA Public Disclosure Data and History pages:
    - Set up .env File:
    - Historical Files: Use 'gnma_historical_downloader.py' to download the complete history for various series. If you want to download a subset of series, comment out the prefixes of the files you do not want in 'dictionary_files/prefix_dictionary.yaml'
    - Current Files: Forthcoming
2. Use the import scripts to load and clean the raw files.
3. Use the cleaned files in your personal research projects.


### ADD NEW DESCRIPTION HERE, KEEPING ABOVE
## Primary functions and flow (high level)

This project implements a simple ETL-style pipeline for GNMA data. The key components are:

- Downloader (`gnma_data_manager/downloader.py`)
  - `GNMAHistoricalDownloader`
    - `download_prefix_data(prefix, start_date=None, end_date=None)`
    - `download_all_data(prefixes=None)`
    - `download_prefix_schemas(prefix)` and `download_all_schemas(prefixes=None)`
    - Utilities: `create_date_suffix(...)`, `get_date_format(...)`, `create_all_prefix_folders(...)`

- Schema Reader (`gnma_data_manager/schema_reader.py`)
  - `GNMASchemaReader`
    - `extract_schemas_to_csv(prefixes=None, augment_with_cobol=True, cobol_column='Remarks')`
    - `combine_schemas(prefix, save_combined_schema=True)` → writes `dictionary_files/combined/<prefix>_combined_schema.csv`
    - `analyze_temporal_coverage()` and `analyze_schema_formats()` (optional reports)
    - COBOL helpers: `parse_cobol_format`, `create_pandas_dtype_mapping`, `create_polars_dtype_mapping`

- Processor (staging + transform) (`gnma_data_manager/processor.py`)
  - `GNMADataProcessor` (single entry point)
    - Staging (normalize raw text/zip → line-parquet in `data/raw`)
      - `stage_prefix(prefix)`
      - `stage_all(exclude_prefixes=None)`
      - `stage_zip_to_parquet(path)` / `stage_txt_to_parquet(path)`
    - Transform (schema-driven, line-parquet → structured parquet in `data/clean`)
      - `transform_prefix(prefix, record_types=None, show_progress=True)`
      - `transform_all(prefixes=None, record_types=None, show_progress=True)`
      - `transform_file(parquet_file, prefix, record_type, output_file, file_format=None)`
      - Helpers: `transform_fixed_width(...)`, `transform_delimited(...)`
    - Orchestration
      - `run_pipeline_for_prefix(prefix, include_formatting=True)`
    - Schema access
      - `load_schema_by_record_type(prefix)`
    - Embedded format detection
      - `detect_file_format(prefix)` (uses combined schema)

- Config (`gnma_data_manager/config.py`)
  - `DownloadConfig`: downloader settings (cookies, base URLs, paths)
  - `ProcessorConfig`: unified settings for staging, transform, and parser (folders, encodings, delimiter, chunk size, etc.)
  - `SchemaReaderConfig`: schema reader paths and behavior

Typical usage
- Extract (download): `GNMAHistoricalDownloader(...).download_prefix_data('dailyllmni')`
- Stage (normalize files): `GNMADataProcessor(...).stage_prefix('dailyllmni')`
- Transform (structured outputs): `GNMADataProcessor(...).transform_prefix('dailyllmni')`
- One-call per-prefix pipeline: `GNMADataProcessor(...).run_pipeline_for_prefix('dailyllmni', include_formatting=True)`

Notes
- Combined schema CSVs are expected in `dictionary_files/combined/<prefix>_combined_schema.csv`.
- Staged line-parquet files are written under `data/raw/<prefix>`.
- Transformed structured parquet files are written under `data/clean/<prefix>/<Record_Type>/`.