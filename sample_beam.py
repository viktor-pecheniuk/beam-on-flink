import logging
from abc import ABC

import apache_beam as beam
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions


# class MyOptions(PipelineOptions):
#     @classmethod
#     def _add_argparse_args(cls, parser):
#         parser.add_argument('--input')
#         parser.add_argument('--output')


class ComputeWordLengthFn(beam.DoFn, ABC):
    def process(self, element):
        print("Print ====> ", element)
        return [element]


def run():
    options = PipelineOptions([
        # "--runner=PortableRunner",
        # "--runner=DirectRunner",
        "--runner=FlinkRunner",
        "--flink_version=1.10",
        "--flink_master=localhost:8081",
        "--environment_type=EXTERNAL",
        "--environment_config=localhost:50000"

        # "--input=inputData.txt",
    ])

    with beam.Pipeline(options=options) as p:

        (p
         | 'ReadMyFile' >> beam.io.ReadFromText('inputData.txt')
         | 'Split words' >> beam.FlatMap(lambda words: words.split(' '))
         | 'Print to console' >> beam.ParDo(ComputeWordLengthFn())
         | 'Write to file' >> WriteToText('test.txt')
         )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
