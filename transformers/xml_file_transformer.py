import re
import apache_beam as beam

from functions.extract_fields_fn import ExtractFields
from utils import parse_into_dict


class StackOverflowDataTransform(beam.PTransform):
    def expand(self, pcoll):
        return (pcoll
                | 'Xml2Dict' >> beam.Map(lambda filename: parse_into_dict(filename))
                | 'Extract' >> beam.ParDo(ExtractFields())
                | 'Group By' >> beam.GroupBy("post_id", "title")
                                    .aggregate_field(lambda row: len(re.findall(r"<.*?>", row.tags)), sum, "tags_count")

                | 'RowsToStr' >> beam.Map(lambda elem: ','.join((elem.post_id, elem.title, str(elem.tags_count))))
                )
