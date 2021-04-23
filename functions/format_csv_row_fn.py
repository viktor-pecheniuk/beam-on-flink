import apache_beam as beam
from apache_beam.metrics import Metrics


class FormatCsvRowFn(beam.DoFn):
    def __init__(self):
        super(FormatCsvRowFn, self).__init__()
        self.num_parse_errors = Metrics.counter(self.__class__, 'num_format_csv_row_errors')

    def process(self, element, *args, **kwargs):
        try:
            csv_row = ','.join((element.post_id, element.title, str(element.tags_count)))
            yield csv_row
        except:
            self.num_parse_errors.inc()
