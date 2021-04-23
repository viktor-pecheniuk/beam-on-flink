from options.common_options import CommonOptions


class FlinkRunnerOptions(CommonOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--runner', default='FlinkRunner')
        parser.add_argument('--flink_version', default='1.10')
        parser.add_argument('--flink_master')
        parser.add_argument('--environment_type', default='EXTERNAL')
        parser.add_argument('--environment_config', default='localhost:50000')
