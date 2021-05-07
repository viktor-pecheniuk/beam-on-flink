import re
import apache_beam as beam

from functions.avro.extract_fields_fn import ExtractFieldsFn
from functions.format_csv_row_fn import FormatCsvRowFn


class StackOverflowAvroDataTransform(beam.PTransform):
    def expand(self, pcoll):
        return (pcoll
                | 'Extract Fields' >> beam.ParDo(ExtractFieldsFn())
                | 'Group By' >> beam.GroupBy("post_id", "title")
                                    .aggregate_field(lambda row: len(row.tags.split("|")), sum, "tags_count")
                | 'Format Result' >> beam.ParDo(FormatCsvRowFn())
                )
