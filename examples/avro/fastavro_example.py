from fastavro import writer, reader, parse_schema
from fastavro.schema import load_schema

# using fastavro library
# code: https://github.com/fastavro/fastavro
# docs: http://fastavro.readthedocs.io/en/latest/

# 2 options to load avro schema:
# a. loading avro schema from a dict containing a JSON schema definition
schema: dict = {
    'doc': 'A weather reading.',
    'name': 'Weather',
    'namespace': 'test',
    'type': 'record',
    'fields': [
        {'name': 'station', 'type': 'string'},
        {'name': 'time', 'type': 'long'},
        {'name': 'temp', 'type': 'int'},
    ],
}
parsed_schema = parse_schema(schema)
# b. loading avro schema from .avsc file
schema_from_file = load_schema("weather.avsc")
assert parsed_schema == schema_from_file

# 'records' can be an iterable (including generator)
records = [
    {u'station': u'011990-99999', u'temp': 0, u'time': 1433269388},
    {u'station': u'011990-99999', u'temp': 22, u'time': 1433270389},
    {u'station': u'011990-99999', u'temp': -11, u'time': 1433273379},
    {u'station': u'012650-99999', u'temp': 111, u'time': 1433275478},
]

# Writing
with open('weather.avro', 'wb') as out:
    writer(out, parsed_schema, records)

more_records = [
    {u'station': u'012390-99999', u'temp': 32, u'time': 1453247388},
    {u'station': u'012701-99999', u'temp': 99, u'time': 1453247560},
]
# Writing: appending to existing file
# Note: When appending, any schema provided will be ignored since the schema in the avro file will be re-used.
# Therefore, it is convenient to just use None as the schema.
with open('weather.avro', 'a+b') as out:
    writer(out, None, more_records)

# Reading
with open('weather.avro', 'rb') as fo:
    for record in reader(fo):
        print(record)
