import sys
import os
from configparser import ConfigParser
import logging
from mod_argparse import setup_cli
from checkers.IndexFile import DocumentIndex, ProgressNoteIndex
from checkers import source_files
from utilities import write_to_file, strip # , write_to_db_isok
# import pandas as pd

logger = logging.getLogger(__name__)
DOCUMENTS = 1
PROGRESS_NOTES = 2

DOC_TYPE = {
    DOCUMENTS: {
        'file_type': 'Document',
        'folder': 'Documents',
        'class': DocumentIndex,
        'log': 'd',
        'dates': ['CREATED_TIMESTAMP', 'POST_DATE'],
        'converters': {'FILENAME': strip,
                       'DISPLAY_DESC': strip,
                       'DOC_COMMENT': strip,
                       'DOCUMENT_TYPE': strip},
        'staff_field': 'displayname'
    },
    PROGRESS_NOTES: {
        'file_type': 'ProgressNote',
        'folder': 'ProgressNotes',
        'class': ProgressNoteIndex,
        'log': 'p',
        'dates': ['CREATED_TIMESTAMP'],
        'converters': {},
        'staff_field': 'login'
    }
}


def folders_with_documents(pat_ids, main_dir_name, doc_prog_folder):
    str_pat_ids = [str(pat_id) for pat_id in pat_ids]
    str_pat_folder_names = [os.path.join(main_dir_name, os.path.join(str_pat_id, doc_prog_folder))
                            for str_pat_id in str_pat_ids]  # Patients/*/Documents

    for pid, folder in zip(str_pat_ids, str_pat_folder_names):
        if os.path.isdir(folder) and len(os.listdir(folder)) > 1:
            yield pid, folder

#
# def write_db_isok(list, pid):
#         df = pd.concat(list)
#         return write_to_db_isok(df, pid)


def df_chunksof_100(df):
    lendf = len(df)

    top100 = df.iloc[:, :100]
    df = df.iloc[:, 100:]
    yield top100, df


def getvalid_src_dest_filepaths(config, pat_ids, main_dir_name, doctype_info):
    idx_chkr_cls = doctype_info['class'].initialize(config)
    log, indx_filename = doctype_info['log'], idx_chkr_cls.__name__ + '.csv'

    idxfile_data = idx_chkr_cls.get_idxfile_data(pid, index_file_path)
    built_src_filenames = idx_chkr_cls.build_src_filenames(idxfile_data)

    for pid, pid_src_folder in folders_with_documents(pat_ids, main_dir_name, doctype_info['folder']):
        index_file_path = os.path.join(pid_src_folder, indx_filename)
        # [ {'staff': 'PETA WRIGHT', 'timestamp': '31/10/2014 10:25:58 AM', 'fileno': '184', 'doctitle': 'Criminal History Zoran', 'ext': 'PDF ', 'pid': '6'}]


        if not idxfile_data:
            logger.debug(f'{pid}{log} | Issue(s) with IndexFile. Skipping..')
            continue
        not_in_indexbuilt_files, not_in_folderlisting = source_files.validate(pid, built_src_filenames,
                                                                              pid_src_folder, log
                                                                              , ignore_file=indx_filename)
        if any([*not_in_indexbuilt_files, *not_in_folderlisting]):
            logger.debug(f'{pid}{log} | invalid folder. skipping')
            continue
        # . . . . . . . . . . . folder listing should match the file names built from index file  . . . . .  . . .
        for idxfile_row, built_src_file in zip(idxfile_data, built_src_filenames):
            destn_filename = idx_chkr_cls.get_destination_filename(idxfile_row)
            if not destn_filename:
                logger.error(f'{pid}{log} | Could not form the destination filename string')
                continue
            logger.info(f'{pid}{log} | src: {built_src_file}  dest:{destn_filename}')
            yield pid, built_src_file, destn_filename


def main():
    config = setup_cli(sys.argv[1:])
    logger.info(f'{sys.argv[0]} -f {config.file_type} -s {config.start_range}'
                f' -e {config.end_range} -d {config.base_directory}')

    docinfo = DOC_TYPE[config.file_type]
    main_dir_name = os.path.join(config.base_directory, 'Patients')
    results_doc = os.path.join(os.path.join(config.base_directory, 'results'), docinfo['folder'])
    logger.debug(f'results doc : {results_doc}')

    parser = ConfigParser()
    parser.read('./config/settings.indexfile.ini')

    outfile = os.path.join('output', f"{docinfo['folder']}.paths_pids.{config.start_range}-{config.end_range}.csv")
    if not os.path.isfile(outfile):
        with open(outfile, 'w', newline='') as csv_file:
            write_to_file("pid", "src", "dest", csv_file, "size")

    #indxfiles_todb(parser, range(config.start_range, config.end_range),
    #                                               main_dir_name, doctype_info)

    with open(outfile, 'a', newline='') as csv_file:
        for p, s, d in getvalid_src_dest_filepaths(parser, range(config.start_range, config.end_range),
                                                   main_dir_name, docinfo):

            full_src_path = f"{main_dir_name}{os.sep}{p}{os.sep}{docinfo['folder']}{os.sep}{s}"
            write_to_file(p, s, d, csv_file, os.path.getsize(full_src_path))


if __name__ == "__main__":
    main()
    # from checkers import sanitize
    # print(sanitize.fix_altdoctitle('None'))
    # print(sanitize.fix_doctitle('None'))
    # print(sanitize.fix_timestamp('None'))
    # print(sanitize.fix_pid('None'))
    # print(sanitize.fix_fileno('None'))
    # print(sanitize.fix_ext('...'))

    # print(sanitize.fix_altdoctitle(None))
    # print(sanitize.fix_doctitle(None))
    # print(sanitize.fix_timestamp(None))
    # print(sanitize.fix_pid(None))
    # print(sanitize.fix_fileno(None))
    # print(sanitize.fix_ext('aa'))
    # print(sanitize.fix_ext('asd'))
    # print(sanitize.fix_ext('bbbb'))
    # print(sanitize.fix_ext('ccccc'))

    # print(sanitize.fix_pid('asdf'))
    # print(sanitize.fix_fileno(3.4))

