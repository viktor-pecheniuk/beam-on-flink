import apache_beam as beam
import xmltodict
from apache_beam.metrics import Metrics


class ParseXmlToDictFn(beam.DoFn):
    def __init__(self):
        super(ParseXmlToDictFn, self).__init__()
        self.num_parse_errors = Metrics.counter(self.__class__, 'num_parse_xml_errors')

    def process(self, element, *args, **kwargs):
        try:
            doc = xmltodict.parse(element)
            yield doc
        except Exception:
            self.num_parse_errors.inc()
