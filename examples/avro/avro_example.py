import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter

# 2 options to load avro schema:
# a. loading avro schema from a string containing a JSON schema definition
schema = """{"namespace": "example.avro",
 "type": "record",
 "name": "User",
 "fields": [
     {"name": "name", "type": "string"},
     {"name": "favorite_number",  "type": ["int", "null"]},
     {"name": "favorite_color", "type": ["string", "null"]}
 ]
}
"""
schema_from_string: avro.schema.Schema = avro.schema.parse(schema)
# b. loading avro schema from .avsc file
schema_from_file: avro.schema.Schema = avro.schema.parse(open("user.avsc", "rb").read())
assert schema_from_string == schema_from_file

# check if datum is valid for that schema - row by row
users: list[dict] = [
    {"name": "Alyssa", "favorite_number": 256},
    {"name": "Ben", "favorite_number": 7, "favorite_color": "red"}
]
for user in users:
    assert schema_from_file.validate(user)

# Serialize users objects and write data into users.avro file.
# DatumWriter is responsible for actually serializing the items to Avro’s binary format
# Schema: DataFileWriter needs the schema both to write the schema to the data file,
# and to verify that the items we write are valid items and write the appropriate fields.
writer = DataFileWriter(open("users.avro", "wb"), DatumWriter(), schema_from_file)
for user in users:
    writer.append(user)
writer.close()

# read created file and de-serialize from avro format
reader = DataFileReader(open("users.avro", "rb"), DatumReader())
for user in reader:
    print(user)
reader.close()
