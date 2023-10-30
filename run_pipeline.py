"""
This script runs the full pipeline
- Step1: creates the Redshift cluster
- Step2: creates tables in the Redshift cluster
- Step3: runs the ETL
- Step4: analytics queries
- Step5: delete Redshift cluster
"""
import argparse
import importlib
import time


STEPS = {
    1: 'create_redshift_cluster',
    2: 'create_tables',
    3: 'etl',
    4: 'analytics_queries',
    5: 'delete_redshift_cluster'
}


def parse_args():
    """
    Parse CLI arguments
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--steps',
        default='1,2,3,4,5',
        help="""
            Define the pipeline steps to be run (default: 1,2,3,4,5)
            1: create_redshift_cluster
            2: create_tables
            3: etl
            4: analytics_queries
            5: delete_redshift_cluster""")
    return parser.parse_args()


def run_module(module_name):
    """Runs main function of a given Python module"""
    module = importlib.import_module(module_name)
    module.main()


def main(args):
    """
    Run pipeline steps set in args.steps
    """
    steps = [int(step) for step in args.steps.split(',')]
    for step in steps:
        start_time = time.time()
        module_name = STEPS[step]
        print('#' * 80)
        print(f'Running {module_name}.py')
        run_module(module_name=module_name)
        elapsed_time = time.time() - start_time
        print(f'Task took {elapsed_time:.1f} sec(s)')


if __name__ == '__main__':
    args = parse_args()
    main(args)
