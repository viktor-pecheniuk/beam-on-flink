import apache_beam as beam

import xmltodict


class ParseXmlToDict(beam.DoFn):
    def process(self, element):
        try:
            doc = xmltodict.parse(element)
        except Exception:
            doc = None
        if doc:
            yield doc
