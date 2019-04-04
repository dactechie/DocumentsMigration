import sys
import os
from configparser import ConfigParser
import logging
from mod_argparse import setup_cli
from checkers.IndexFile import DocumentIndex, ProgressNoteIndex
from utilities import copy_using_output_csv


logger = logging.getLogger(__name__)

DOCUMENTS = 1
PROGRESS_NOTES = 2

DOC_TYPE = {
    DOCUMENTS: {
        'file_type': 'Document',
        'folder': 'Documents',
        'class': DocumentIndex,
        'log': 'd'
    },
    PROGRESS_NOTES: {
        'file_type': 'ProgressNote',
        'folder': 'ProgressNotes',
        'class': ProgressNoteIndex,
        'log': 'p'
    }
}


def main():
    config = setup_cli(sys.argv[1:])
    logger.info(f'{sys.argv[0]} -f {config.file_type} -s {config.start_range}'
                f' -e {config.end_range} -d {config.base_directory}')

    doctype_info = DOC_TYPE[config.file_type]
    base_dir = config.base_directory
    main_dir_name = os.path.join(base_dir, 'Patients')
    results_doc = os.path.join(r'.\results', doctype_info['folder'])
    logger.debug(f'results doc : {results_doc}')

    parser = ConfigParser()
    parser.read('./config/settings.indexfile.ini')

    datafile = os.path.join('output',
                            f"{doctype_info['folder']}.paths_pids.{config.start_range}-{config.end_range}.csv")

    copy_using_output_csv(datafile, main_dir_name, results_doc, doctype_info['folder'])


if __name__ == "__main__":
    main()


