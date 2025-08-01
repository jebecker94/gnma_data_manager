"""
GNMA Data Parser

Intelligent parser that automatically detects and handles both delimited and fixed-width
GNMA data files based on their schema structure.
"""

import pandas as pd
import re
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Union
import numpy as np


class GNMADataParser:
    """
    Intelligent parser for GNMA data files that automatically detects format type
    and applies appropriate parsing strategy.
    """
    
    def __init__(self, verbose: bool = False):
        self.verbose = verbose
        self.schema_cache = {}
        
    def detect_file_format(self, schema_df: pd.DataFrame) -> str:
        """
        Detect whether a file is delimited or fixed-width based on schema structure.
        
        Args:
            schema_df: DataFrame containing the schema definition
            
        Returns:
            str: 'DELIMITED', 'FIXED_WIDTH', or 'UNKNOWN'
        """
        columns = list(schema_df.columns)
        
        has_max_length = 'Max Length' in columns
        has_begin_end = 'Begin' in columns and 'End' in columns
        has_format = 'Format' in columns
        has_description = 'Description' in columns or 'Definition' in columns
        
        if has_max_length and has_format:
            return 'DELIMITED'
        elif has_begin_end:
            return 'FIXED_WIDTH'
        else:
            return 'UNKNOWN'
    
    def detect_delimiter(self, file_path: Path, schema_df: pd.DataFrame) -> str:
        """
        Detect the delimiter used in a delimited file.
        
        Args:
            file_path: Path to the data file
            schema_df: Schema DataFrame
            
        Returns:
            str: Detected delimiter ('|', ',', '\t', etc.)
        """
        try:
            # Read first few lines to analyze
            with open(file_path, 'r', encoding='utf-8') as f:
                sample_lines = [f.readline().strip() for _ in range(5)]
            
            # Check common delimiters
            delimiters = ['|', ',', '\t', ';']
            expected_fields = len(schema_df)
            
            for delimiter in delimiters:
                field_counts = []
                for line in sample_lines:
                    if line:
                        field_counts.append(len(line.split(delimiter)))
                
                if field_counts and all(count == expected_fields for count in field_counts):
                    if self.verbose:
                        print(f"   Detected delimiter: '{delimiter}' ({expected_fields} fields)")
                    return delimiter
            
            # Default to pipe if uncertain
            if self.verbose:
                print(f"   Using default delimiter: '|'")
            return '|'
            
        except Exception as e:
            if self.verbose:
                print(f"   Error detecting delimiter: {e}")
            return '|'
    
    def parse_fixed_width_file(self, file_path: Path, schema_df: pd.DataFrame) -> pd.DataFrame:
        """
        Parse a fixed-width data file using Begin/End column positions.
        
        Args:
            file_path: Path to the data file
            schema_df: Schema DataFrame with Begin/End columns
            
        Returns:
            pd.DataFrame: Parsed data
        """
        if self.verbose:
            print(f"   Parsing as FIXED-WIDTH format...")
        
        try:
            # Create column specifications
            colspecs = []
            column_names = []
            
            # Group by record type if available
            if 'Record_Type' in schema_df.columns:
                record_types = schema_df['Record_Type'].dropna().unique()
                if self.verbose:
                    print(f"   Found {len(record_types)} record types: {list(record_types)}")
            
            # Use the first record type or all rows if no record types
            if 'Record_Type' in schema_df.columns and len(schema_df['Record_Type'].dropna()) > 0:
                # Get schema for first record type
                first_record_type = schema_df['Record_Type'].dropna().iloc[0]
                schema_subset = schema_df[schema_df['Record_Type'] == first_record_type].copy()
            else:
                schema_subset = schema_df.copy()
            
            # Build column specifications
            for _, row in schema_subset.iterrows():
                if pd.notna(row['Begin']) and pd.notna(row['End']):
                    begin = int(row['Begin']) - 1  # Convert to 0-based indexing
                    end = int(row['End'])
                    
                    colspecs.append((begin, end))
                    
                    # Clean column name
                    col_name = str(row.get('Data Item', row.get('Data Element', f'Field_{row["Item"]}')))
                    col_name = re.sub(r'[^\w\s]', '', col_name).strip()
                    col_name = re.sub(r'\s+', '_', col_name)
                    column_names.append(col_name)
            
            if not colspecs:
                raise ValueError("No valid Begin/End columns found in schema")
            
            # Read the fixed-width file
            df = pd.read_fwf(
                file_path,
                colspecs=colspecs,
                names=column_names,
                dtype=str  # Read as strings initially
            )
            
            if self.verbose:
                print(f"   Successfully parsed {len(df)} rows, {len(df.columns)} columns")
            
            return df
            
        except Exception as e:
            if self.verbose:
                print(f"   Error parsing fixed-width file: {e}")
            return pd.DataFrame()
    
    def parse_delimited_file(self, file_path: Path, schema_df: pd.DataFrame) -> pd.DataFrame:
        """
        Parse a delimited data file using schema column definitions.
        
        Args:
            file_path: Path to the data file
            schema_df: Schema DataFrame with Max Length columns
            
        Returns:
            pd.DataFrame: Parsed data
        """
        if self.verbose:
            print(f"   Parsing as DELIMITED format...")
        
        try:
            # Detect delimiter
            delimiter = self.detect_delimiter(file_path, schema_df)
            
            # Create column names from schema
            column_names = []
            for _, row in schema_df.iterrows():
                col_name = str(row.get('Data Element', row.get('Data Item', f'Field_{row["Item"]}')))
                # Clean column name
                col_name = re.sub(r'[^\w\s]', '', col_name).strip()
                col_name = re.sub(r'\s+', '_', col_name)
                column_names.append(col_name)
            
            # Read the delimited file
            df = pd.read_csv(
                file_path,
                delimiter=delimiter,
                names=column_names,
                dtype=str,  # Read as strings initially
                header=None,
                on_bad_lines='skip'
            )
            
            if self.verbose:
                print(f"   Successfully parsed {len(df)} rows, {len(df.columns)} columns")
            
            return df
            
        except Exception as e:
            if self.verbose:
                print(f"   Error parsing delimited file: {e}")
            return pd.DataFrame()
    
    def apply_data_types(self, df: pd.DataFrame, schema_df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply appropriate data types based on schema Type column.
        
        Args:
            df: Parsed DataFrame
            schema_df: Schema DataFrame with Type information
            
        Returns:
            pd.DataFrame: DataFrame with applied data types
        """
        if df.empty or 'Type' not in schema_df.columns:
            return df
        
        df_typed = df.copy()
        
        try:
            for i, (_, schema_row) in enumerate(schema_df.iterrows()):
                if i >= len(df.columns):
                    break
                    
                col_name = df.columns[i]
                data_type = str(schema_row.get('Type', '')).lower()
                
                if 'numeric' in data_type or 'number' in data_type:
                    df_typed[col_name] = pd.to_numeric(df_typed[col_name], errors='coerce')
                elif 'date' in data_type:
                    df_typed[col_name] = pd.to_datetime(df_typed[col_name], errors='coerce')
                # Character fields stay as strings
            
            if self.verbose:
                print(f"   Applied data types from schema")
                
        except Exception as e:
            if self.verbose:
                print(f"   Warning: Could not apply all data types: {e}")
        
        return df_typed
    
    def parse_data_file(self, data_file_path: Path, schema_file_path: Path) -> pd.DataFrame:
        """
        Parse a GNMA data file using its corresponding schema.
        
        Args:
            data_file_path: Path to the data file
            schema_file_path: Path to the schema CSV file
            
        Returns:
            pd.DataFrame: Parsed data
        """
        if self.verbose:
            print(f"\nParsing: {data_file_path.name}")
            print(f"   Schema: {schema_file_path.name}")
        
        try:
            # Load schema
            schema_df = pd.read_csv(schema_file_path)
            
            if schema_df.empty:
                if self.verbose:
                    print(f"   Error: Empty schema file")
                return pd.DataFrame()
            
            # Detect format
            file_format = self.detect_file_format(schema_df)
            
            if self.verbose:
                print(f"   Detected format: {file_format}")
            
            # Parse based on format
            if file_format == 'DELIMITED':
                df = self.parse_delimited_file(data_file_path, schema_df)
            elif file_format == 'FIXED_WIDTH':
                df = self.parse_fixed_width_file(data_file_path, schema_df)
            else:
                if self.verbose:
                    print(f"   Error: Unknown format - cannot parse")
                return pd.DataFrame()
            
            # Apply data types
            if not df.empty:
                df = self.apply_data_types(df, schema_df)
            
            return df
            
        except Exception as e:
            if self.verbose:
                print(f"   Error parsing file: {e}")
            return pd.DataFrame()
    
    def parse_data_with_prefix_schema(self, data_file_path: Path, prefix: str) -> pd.DataFrame:
        """
        Parse a data file using the combined schema for its prefix.
        
        Args:
            data_file_path: Path to the data file
            prefix: File prefix to find the appropriate schema
            
        Returns:
            pd.DataFrame: Parsed data
        """
        schema_file_path = Path(f'dictionary_files/combined/{prefix}_combined_schema.csv')
        
        if not schema_file_path.exists():
            if self.verbose:
                print(f"Error: Schema file not found: {schema_file_path}")
            return pd.DataFrame()
        
        return self.parse_data_file(data_file_path, schema_file_path)


def demo_parser(prefix: str = 'nimonSFPS', verbose: bool = True):
    """
    Demonstrate the parser with a specific prefix.
    
    Args:
        prefix: Prefix to analyze
        verbose: Whether to print detailed output
    """
    parser = GNMADataParser(verbose=verbose)
    
    # Load schema for analysis
    schema_file = Path(f'dictionary_files/combined/{prefix}_combined_schema.csv')
    
    if not schema_file.exists():
        print(f"Error: Schema file not found: {schema_file}")
        return
    
    print(f"Analyzing schema format for: {prefix}")
    
    try:
        schema_df = pd.read_csv(schema_file)
        file_format = parser.detect_file_format(schema_df)
        
        print(f"Schema structure:")
        print(f"   Columns: {list(schema_df.columns)}")
        print(f"   Rows: {len(schema_df)}")
        print(f"   Detected format: {file_format}")
        
        if 'Record_Type' in schema_df.columns:
            record_types = schema_df['Record_Type'].dropna().unique()
            print(f"   Record types: {list(record_types)}")
        
        # Show sample schema rows
        print(f"\nSample schema entries:")
        sample_cols = ['Item', 'Data Element', 'Data Item', 'Type', 'Max Length', 'Length', 'Begin', 'End']
        available_cols = [col for col in sample_cols if col in schema_df.columns]
        print(schema_df[available_cols].head(3).to_string(index=False))
        
    except Exception as e:
        print(f"Error analyzing schema: {e}")


def analyze_all_schemas(verbose: bool = False) -> pd.DataFrame:
    """
    Analyze all available schemas and categorize them by format type.
    
    Args:
        verbose: Whether to print detailed output
        
    Returns:
        pd.DataFrame: Summary of all schemas with format classifications
    """
    parser = GNMADataParser(verbose=False)
    combined_dir = Path('dictionary_files/combined')
    
    if not combined_dir.exists():
        print(f"Error: Combined schemas directory not found: {combined_dir}")
        return pd.DataFrame()
    
    schema_files = list(combined_dir.glob('*_combined_schema.csv'))
    
    if not schema_files:
        print(f"Error: No combined schema files found")
        return pd.DataFrame()
    
    results = []
    delimited_count = 0
    fixed_width_count = 0
    unknown_count = 0
    
    if verbose:
        print(f"Analyzing {len(schema_files)} schema files...\n")
    
    for schema_file in schema_files:
        prefix = schema_file.stem.replace('_combined_schema', '')
        
        try:
            schema_df = pd.read_csv(schema_file)
            
            if schema_df.empty:
                continue
            
            # Detect format
            file_format = parser.detect_file_format(schema_df)
            columns = list(schema_df.columns)
            
            # Count formats
            if file_format == 'DELIMITED':
                delimited_count += 1
            elif file_format == 'FIXED_WIDTH':
                fixed_width_count += 1
            else:
                unknown_count += 1
            
            # Analyze schema structure
            has_record_types = 'Record_Type' in schema_df.columns
            record_type_count = 0
            if has_record_types:
                record_type_count = len(schema_df['Record_Type'].dropna().unique())
            
            # Get key format indicators
            format_indicators = []
            if 'Max Length' in columns:
                format_indicators.append('Max Length')
            if 'Length' in columns:
                format_indicators.append('Length')
            if 'Begin' in columns and 'End' in columns:
                format_indicators.append('Begin/End')
            if 'Format' in columns:
                format_indicators.append('Format')
            if 'Description' in columns:
                format_indicators.append('Description')
            if 'Definition' in columns:
                format_indicators.append('Definition')
            
            results.append({
                'Prefix': prefix,
                'Format_Type': file_format,
                'Schema_Rows': len(schema_df),
                'Schema_Columns': len(columns),
                'Has_Record_Types': has_record_types,
                'Record_Type_Count': record_type_count,
                'Key_Indicators': ', '.join(format_indicators),
                'Primary_Field_Name': columns[1] if len(columns) > 1 else 'Unknown'  # Usually Data Item or Data Element
            })
            
            if verbose:
                status = "OK" if file_format in ['DELIMITED', 'FIXED_WIDTH'] else "UNKNOWN"
                print(f"{status} {prefix:<20} | {file_format:<12} | {len(schema_df):>4} rows | {record_type_count:>2} types")
                
        except Exception as e:
            if verbose:
                print(f"ERROR {prefix:<20} | ERROR: {str(e)}")
            results.append({
                'Prefix': prefix,
                'Format_Type': 'ERROR',
                'Schema_Rows': 0,
                'Schema_Columns': 0,
                'Has_Record_Types': False,
                'Record_Type_Count': 0,
                'Key_Indicators': f'Error: {str(e)}',
                'Primary_Field_Name': 'Unknown'
            })
    
    # Convert to DataFrame
    results_df = pd.DataFrame(results)
    
    if verbose and not results_df.empty:
        print(f"\n" + "="*80)
        print(f"SCHEMA FORMAT ANALYSIS SUMMARY")
        print(f"="*80)
        print(f"Total Schemas: {len(results_df)}")
        print(f"Delimited Files: {delimited_count}")
        print(f"Fixed-Width Files: {fixed_width_count}")
        print(f"Unknown/Error: {unknown_count}")
        print(f"="*80)
    
    return results_df


def create_format_mapping() -> Dict[str, str]:
    """
    Create a simple mapping of prefix to format type for easy lookup.
    
    Returns:
        Dict[str, str]: Mapping of prefix to format type
    """
    analysis_df = analyze_all_schemas(verbose=False)
    
    if analysis_df.empty:
        return {}
    
    # Filter out errors and unknowns
    valid_df = analysis_df[analysis_df['Format_Type'].isin(['DELIMITED', 'FIXED_WIDTH'])]
    
    return dict(zip(valid_df['Prefix'], valid_df['Format_Type']))


def get_parsing_strategy(prefix: str) -> str:
    """
    Get the recommended parsing strategy for a given prefix.
    
    Args:
        prefix: File prefix
        
    Returns:
        str: Parsing strategy description
    """
    format_mapping = create_format_mapping()
    
    if prefix not in format_mapping:
        return "Unknown format - manual analysis required"
    
    format_type = format_mapping[prefix]
    
    if format_type == 'DELIMITED':
        return ("Use delimited parser with delimiter detection. "
                "Look for pipe (|), comma, or tab separation. "
                "Column names from Data Element field.")
    elif format_type == 'FIXED_WIDTH':
        return ("Use fixed-width parser with Begin/End positions. "
                "Column names from Data Item field. "
                "Handle multiple record types separately.")
    else:
        return "Format analysis inconclusive"


if __name__ == "__main__":
    # Demo with different format types
    print("="*80)
    print("DELIMITED FORMAT EXAMPLE")
    print("="*80)
    demo_parser('nimonSFPS')
    
    print("\n" + "="*80)
    print("FIXED-WIDTH FORMAT EXAMPLE") 
    print("="*80)
    demo_parser('dailyllmni') 