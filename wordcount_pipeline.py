import argparse
import logging
import re

import apache_beam as beam

from options.common_options import CommonOptions
from options.dataflow_runner_options import DataflowRunnerOptions
from options.flink_runner_options import FlinkRunnerOptions


class WordExtractingDoFn(beam.DoFn):
    """Parse each line of input text into words."""

    def process(self, element):
        """Returns an iterator over the words of this element.
        The element is a line of text.  If the line is blank, note that, too.
        Args:
          element: the element being processed
        Returns:
          The processed element.
        """
        return re.findall(r'[\w\']+', element, re.UNICODE)


def format_result(word, count):
    return '%s: %d' % (word, count)


def run(options):
    with beam.Pipeline(options=options) as p:
        lines = (p | 'Read test xml file' >> beam.io.ReadFromText(p.options.input, validate=False))

        counts = (
                lines
                | 'Split' >> (beam.ParDo(WordExtractingDoFn()).with_output_types(str))
                | 'PairWIthOne' >> beam.Map(lambda x: (x, 1))
                | 'GroupAndSum' >> beam.CombinePerKey(sum))

        output = counts | 'Format' >> beam.MapTuple(format_result)

        output | 'Write' >> beam.io.WriteToText(p.options.output)


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    parser = argparse.ArgumentParser(description='Process sample dataset by Apache Beam.')
    parser.add_argument('--runner', type=str, help='select `Runner` for the pipeline')
    args = parser.parse_known_args()

    if args[0].runner == "flink":
        run(options=FlinkRunnerOptions())
    elif args[0].runner == "dataflow":
        run(options=DataflowRunnerOptions())
    elif args[0].runner == "direct":
        run(options=CommonOptions())
    # TODO add case for PortableRunner
    else:
        run(options=CommonOptions())
