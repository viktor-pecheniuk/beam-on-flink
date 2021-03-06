import re

import apache_beam as beam

from functions.xml.extract_fields_fn import ExtractFieldsFn
from functions.xml.parse_xml_fn import ParseXmlToDictFn
from functions.format_csv_row_fn import FormatCsvRowFn


class StackOverflowXmlDataTransform(beam.PTransform):
    def expand(self, pcoll):
        return (pcoll
                | 'Xml2Dict' >> beam.ParDo(ParseXmlToDictFn())
                | 'Extract Fields' >> beam.ParDo(ExtractFieldsFn())
                | 'Group By' >> beam.GroupBy("post_id", "title")
                                    .aggregate_field(lambda row: len(re.findall(r"<.*?>", row.tags)), sum, "tags_count")
                | 'Format Result' >> beam.ParDo(FormatCsvRowFn())
                )
