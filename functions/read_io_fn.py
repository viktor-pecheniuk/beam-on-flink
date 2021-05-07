import apache_beam as beam
import fastavro

from apache_beam.io.filesystems import FileSystems


class ReadLines(beam.DoFn):
    def process(self, element, *args, **kwargs):
        with FileSystems.open(element) as gcs_file:
            for line in gcs_file:
                yield line
            # avro_reader = fastavro.reader(gcs_file)
            # for row in avro_reader:
            #     csv_row = ','.join((str(row['id']), row['title'], row['tags']))
            #     yield csv_row
