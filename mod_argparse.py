import argparse
import sys


def setup_cli(args1):

    parser = argparse.ArgumentParser(description='A tool for copying and renaming files with specific biz logic.')
    parser.add_argument('-f', '--file_type', choices=[1, 2], type=int, default=1,
                        help="--file_type 1: Documents, 2: ProgressNotes", nargs='?')
    parser.add_argument('-s', '--start_range', type=int, default=1,
                        help="Start of the Pat-id folder range to process",
                        nargs='?')

    parser.add_argument('-e', '--end_range', type=int, default=2,
                        help="End of the Pat-id folder range to process",
                        nargs='?')

    parser.add_argument('-d', '--base_directory', default=".",
                        help="Root/base directory to start processing from/drop results to. "
                             "Expect: 'Patients' and 'results' directory subdirectories and *Index.csv",
                        nargs='?')

    return parser.parse_args(args1)


if __name__ == '__main__':
    args = setup_cli(sys.argv[1:])
    print(args.file_type)
    print(args.start_range)
    print(args.end_range)
    print(args.base_directory)
