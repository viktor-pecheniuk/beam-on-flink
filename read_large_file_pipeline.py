import argparse
import logging

import apache_beam as beam

from functions.read_io_fn import ReadLines

from options.common_options import CommonOptions
from options.dataflow_runner_options import DataflowRunnerOptions
from options.flink_runner_options import FlinkRunnerOptions


def run(options):
    with beam.Pipeline(options=options) as p:
        (p
         | 'Create' >> beam.Create([p.options.input])
         | 'Read lines' >> beam.ParDo(ReadLines())
         | 'Write to file' >> beam.io.WriteToText(
                    p.options.output,
                    file_name_suffix=".csv",
                    header=p.options.csv_header
                )
         )


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
