from apache_beam.options.pipeline_options import PipelineOptions


class CommonOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--input')
        parser.add_argument('--output')
        parser.add_argument('--csv_header')
