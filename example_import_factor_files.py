# Import Packages
import polars as pl

# Set File Name
fwf_name = './data/raw/factorAplat_202001.txt'

# Load the Factor Data
df = pl.read_csv(
    fwf_name,
    has_header=False,
    new_columns=["full_str"]
)

# Read Dictionary File
dictionary_file = './dictionary_files/clean/factor_layouts_combined.csv'
formats = pl.read_csv(dictionary_file)
formats = formats.filter(pl.col('Prefix')=="factorAplat")
column_names = formats.select(pl.col('Data Item')).to_series().to_list()
widths = formats.select(pl.col('Length')).to_series().to_list()

# Fix Column Names for Filler Values
filler_count = 0
final_column_names = []
for column_name in column_names :
    if column_name == 'Filler' :
        column_name += f' {filler_count+1}'
        filler_count += 1
    final_column_names.append(column_name)

# Calculate slice values from widths.
slice_tuples = []
offset = 0
for i in widths:
    slice_tuples.append((offset, i))
    offset += i

# Create Final DataFrame (and drop full string)
df = df.with_columns(
    [
       pl.col("full_str").str.slice(slice_tuple[0], slice_tuple[1]).str.strip_chars().alias(col)
       for slice_tuple, col in zip(slice_tuples, final_column_names)
    ]
).drop("full_str")
