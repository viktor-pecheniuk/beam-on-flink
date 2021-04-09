from options.common_options import CommonOptions


class DataflowRunnerOptions(CommonOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--runner', default='DataflowRunner')
        parser.add_argument('--project')
        parser.add_argument('--region')
        parser.add_argument('--temp_location')
        parser.add_argument('--staging_location')
