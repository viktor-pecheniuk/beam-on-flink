from fastavro import reader, is_avro

file_path = "/Users/vpeche/tmp/avro/000000000000.AVRO"
print("File is AVRO: {}".format(is_avro(file_path)))

with open(file_path, 'rb') as fo:
    avro_reader = reader(fo)
    for record in avro_reader:
        print(record)
