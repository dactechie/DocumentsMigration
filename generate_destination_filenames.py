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
#
# def indxfiles_todb(config, pat_ids, main_dir_name, doctype_info):
#     idx_chkr_cls = doctype_info['class'].initialize(config)
#     log, indx_filename = doctype_info['log'], idx_chkr_cls.__name__ + '.csv'
#
#     # df = pd.DataFrame()
#     list_ = []
#     pid_start = 0
#     pid_end = 0
#     df_rows = 0
#     # https://stackoverflow.com/questions/50689082/to-sql-pyodbc-count-field-incorrect-or-syntax-error
#     skipped_list = []
#     for pid, pid_src_folder in folders_with_documents(pat_ids, main_dir_name, doctype_info['folder']):
#         index_file_path = os.path.join(pid_src_folder, indx_filename)
#         #int_pid = int(pid)
#         pid_end = int(pid)
#         try:
#             df = pd.read_csv(index_file_path, index_col=None, header=0,
#                              converters=doctype_info['converters'],
#                              # date_parser=pd.core.tools.datetimes.to_datetime,
#                              # parse_dates=doctype_info['dates']
#                              )
#             df['PID'] = pid
#
#             #len_df = len(df)
#
#             # if len_df > 50:
#             #     df_1 = pd.concat(list_)
#             #     if write_to_db_isok(df_1, f'{pid_start} - {pid_end}'):
#             #         logger.info(f'Wrote to DB..upto pid: {pid}  dfrows: {df_rows}')
#             #     else:
#             #         logger.error(f'Skipping batch because of errors : {pid_start} - {pid_end}  dfrows: {df_rows} ')
#             #         skipped_list.extend(range(pid_start, pid_end))
#             #     pid_start = pid_end + 1
#             #
#             # else:
#             # list_.append(df)
#             # df_rows += len_df
#
#             list_.append(df)
#             df_rows += len(df)
#             if len(list_) > 5 or df_rows > 30:
#                 df = pd.concat(list_)
#                 if write_to_db_isok(df, f'{pid_start} - {pid_end}'):
#                     logger.info(f'Wrote to DB..upto pid: {pid}  dfrows: {df_rows}')
#                 else:
#                     logger.error(f'Skipping batch because of errors : {pid_start} - {pid_end}  dfrows: {df_rows} ')
#                     skipped_list.extend(range(pid_start, pid_end))
#                 list_ = []
#                 pid_start = pid_end+1
#                 df_rows = 0
#
#         except pd.errors.ParserError as pe:
#             logger.error(f"Something went wrong (ParseError) pid:{pid}. skipping..")
#             list_ = []
#             pid_start = pid_end + 1
#             df_rows = 0
#             continue
#
#     if list_:
#         df = pd.concat(list_)
#         if write_to_db_isok(df, {pid_start} - {pid_end}):
#             logger.info(f'Wrote to DB..upto pid: {pid}')
#
#     logger.warning(f'Skipped list of pids: {",".join([str(i) for i in skipped_list])}')


def getvalid_src_dest_filepaths(config, pat_ids, main_dir_name, doctype_info):
    idx_chkr_cls = doctype_info['class'].initialize(config)
    log, indx_filename = doctype_info['log'], idx_chkr_cls.__name__ + '.csv'

    for pid, pid_src_folder in folders_with_documents(pat_ids, main_dir_name, doctype_info['folder']):
        index_file_path = os.path.join(pid_src_folder, indx_filename)
        idxfile_data, built_src_filenames = idx_chkr_cls.get_idxfile_data(pid, index_file_path)
        if not idxfile_data:
            logger.debug(f'{pid}{log} | Issue(s) with IndexFile. Skipping..')
            continue
        not_in_indexbuilt_files, not_in_folderlisting = source_files.validate(pid, built_src_filenames,
                                                                              pid_src_folder, log, indx_filename)
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

