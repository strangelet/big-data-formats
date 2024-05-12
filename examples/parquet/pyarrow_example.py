import pyarrow as pa
import pyarrow.parquet as pq

# Schema for all fields
my_schema = pa.schema([('name', pa.string()),
                       ('favorite_number', pa.int32()),
                       ('favorite_color', pa.string())])

list_people = [
    {"name": "Alyssa", "favorite_number": 256},
    {"name": "Ben", "favorite_number": 7, "favorite_color": "red"},
    {"name": "Carl", "favorite_number": 69, "favorite_color": "blue"},
    {"name": "Diane", "favorite_number": 256, "favorite_color": "black"},
]

table: pa.Table = pa.Table.from_pylist(list_people, schema=my_schema)
print("\n\nExample dataset:")
print(table)

# Writing the example to a single parquet file
pq.write_table(table=table, where='example_single_file.parquet')
# Reading back the created file
table2: pa.Table = pq.read_table('example_single_file.parquet')
print("\n\nRead table from 1 parquet file:")
print(table2)

# Partitioned Datasets (Multiple Files)
# Writing with automatic partitioning of data
pq.write_to_dataset(table=table, root_path='partitioned', partition_cols=['favorite_number'])

# Reading the partitioned dataset from filesystem
dataset = pq.ParquetDataset('partitioned/')
table3: pa.Table = dataset.read(columns=['name', 'favorite_number', 'favorite_color'])
print("\n\nRead dataset from partitioned parquet files:")
type(table3)
print(table3)
