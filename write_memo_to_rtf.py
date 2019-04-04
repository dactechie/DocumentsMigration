import os
import sys
import csv
import click
import signal
import logging
import datetime
from collections import OrderedDict
import pandas as pd



# import numpy as np

# python.exe .\write_memo_to_rtf.py  -b "S:\Directions Central\Mastercare Development\MastercareImplementation\Migration in progress data\Migration Patient Documents" -s 1 -e 200

# Get-Content C:\Users\aftab.jalal\PycharmProjects\DocsMigration\logs\debug_rtf_..-write_memo_to_rtf.log -Wait

logging.basicConfig(filename='logs\\debug_rtf_{}-{}.log'.
                    format(datetime.datetime.now().strftime('%Y%m%d_%H%M%S'), os.path.basename(__file__)[:-3]),
                    level=logging.DEBUG,
                    format='%(message)s')
                    #format='%(asctime)s -  %(levelname)s - %(message)s')    # - %(name)s - %(threadName)s

logger = logging.getLogger("rtf_logger")

not_empty = []
index_not_aware = []
file_nonexistent = []

memo_field = 'PAT_MORB_DESC_MEMO'
memo_type_field = 'MORB_TYPE_SHORT_DESC'

file_header = r'{\rtf1'
line_prefix = r'\par '
file_footer = '\r' + r'\par \par}'


def log_results():
    if len(index_not_aware) > 0:    # index_not_aware : patID_encNO
        logger.debug("ProgNotesIndex not aware of pat_id + enc_no : {}".format(','.join(index_not_aware)))
    if len(not_empty) > 0:
        logger.debug("File is not empty . skipping {} ".format(','.join(not_empty)))
    if len(file_nonexistent) > 0:
        logger.debug("File(s) don't exist; creating..{} ".format(','.join(file_nonexistent)))


def signal_handler(signal, frame):
    print('You pressed Ctrl+C!. Logging Results...')
    log_results()
    sys.exit(0)


# the index file is sorted by pat_id so can break out once reached the end_patid
def setup_fileindex_numbers(prog_notes_file_index, start_patid, end_patid):
    ids_dict = OrderedDict()
    with open(file=prog_notes_file_index, mode='r') as rf:
        reader = csv.DictReader(rf)

        # prog_idx_rows = [row for row in reader if end_patid > int(row['PAT_ID']) >= start_patid]
        # idxs = list(map(lambda p:  ids_dict[p['PAT_ID'] + '_' + p['ENC_NO']]=1, prog_idx_rows))

        for row in reader:
            pat_id = int(row['PAT_ID'])
            if end_patid > pat_id >= start_patid:
                ids_dict[row['PAT_ID'] + '_' + row['ENC_NO']] = {
                                        'PAT_ID': row['PAT_ID'],
                                        'ENC_NO': row['ENC_NO'],
                                        'PROG_NOTE_NO': row['PROGRESS_NOTE_NO']
                                        }
            elif pat_id >= end_patid:
                break

    return ids_dict


def write_rtf(text, file):
    file.write('{\\rtf1')
    memo_text_lines = text.splitlines()

    for line in memo_text_lines:
        file.write(line_prefix + line.strip())
    file.write('}')


"""
    Per PID prog note details
"""


def progno_timestamps_user(prog_index_file):
    ptu = {}
    with open(file=prog_index_file, mode='r') as rf:
        reader = csv.DictReader(rf)
        for row in reader:
            dt = datetime.datetime.strptime(row['CREATED_TIMESTAMP'], '%d/%m/%Y %I:%M:%S %p')
            ptu[row['PROGRESS_NOTE_NO']] = {
                'timestamp': str(dt).replace(':', '.'),
                'staff': row['CREATED_USER'].title()
            }
    return ptu


'''
    A generator that returns the destination filename and the contents to write into it.
    Input parameter mdf is the morbidity data frame which comes from CCARE export into morbidity.csv 
'''
def get_all_filename_text(ids_dict, mdf, results_dir, base_directory):
    relevant_patids = mdf.PAT_ID.unique()
    logger.info('pid, size, [files] : ')
    pat_dir = os.path.join(base_directory, 'Patients')
    for pid in relevant_patids:
        prog_notes_files_for_patid = []
        tot_size_for_patid = 0

        # PatientMorbiditySorted : Text column is constructed as follows :
        # =IF(LEN(AR2&AQ2)=0,Y2,IF(LEN(AR2)>0,AR2,AQ2))
        df = mdf[(mdf.PAT_ID == pid) & mdf['ENC_NO'] > 0]
        text = df.groupby('ENC_NO').apply(lambda x: '\n'.join(x.Text))

        prog_indx_file = os.path.join(os.path.join(os.path.join(pat_dir,  str(pid)), 'ProgressNotes'),
                                      'ProgressNoteIndex.csv')
        ptu = progno_timestamps_user(prog_indx_file)

        pid_str = str(pid)
        for enc_no, text in text.iteritems():  # for each enc_no
            pid_enc = f'{pid_str}_{str(enc_no)}'
            if pid_enc not in ids_dict:
                index_not_aware.append(pid_enc)
                continue

            tu = ptu[ids_dict[pid_enc]['PROG_NOTE_NO']]

            prog_notes_filename1 = f"{pid_str}_{tu['staff']}_{tu['timestamp']}_{ids_dict[pid_enc]['PROG_NOTE_NO']}"
            prog_notes_filename = os.path.join(results_dir, prog_notes_filename1 + '.rtf')
            if not os.path.isfile(prog_notes_filename):
                file_nonexistent.append(prog_notes_filename1)
            elif os.stat(prog_notes_filename).st_size > 0:
                not_empty.append(prog_notes_filename1)
                continue

            text = f"{tu['staff']} @ {tu['timestamp']} \n {text} \n"

            prog_notes_files_for_patid.append(ids_dict[pid_enc]['PROG_NOTE_NO'])    # prog_notes_filename1)
            tot_size_for_patid += len(text)

            yield prog_notes_filename, text

        if len(prog_notes_files_for_patid) > 0:
            logger.debug(f'{pid}, {round(tot_size_for_patid/1024, 2)}, {",".join(prog_notes_files_for_patid)}')
                #'{},{},{}'.
                #         format(pid, round(tot_size_for_patid/1024, 2), ','.join(prog_notes_files_for_patid)))


def conv_nan_zero(val):
    if val == '':   # val == np.nan or
        return 0
    return val


'''
    Get a Pandas DataFrame from the Morbidity csv file (for easier handling)
'''
def df_from_file(morbidity_csv, start_patid, end_patid):
    iter_csv = pd.read_csv(morbidity_csv
                           , encoding='cp1252'
                           , iterator=True
                           , chunksize=10000
                           , converters={'ENC_NO': conv_nan_zero}
                           , low_memory=False)
    chunks = []
    for chunk in iter_csv:
        # sorted PIDs, so no need to do: all(chunk['PAT_ID'] >= end_range):
        last_pid = chunk['PAT_ID'][-1:].values[0]
        # we need to skip some to get to the  range we are interested in  (last pid of chunk < start)
        if last_pid < start_patid:
            continue

        first_pid = chunk['PAT_ID'].iloc[0]
        # we've gone beyond the range of interest. first-pid of chunk > end (sorted, so we can quit)
        if first_pid >= end_patid:
            break
        elif last_pid < end_patid and first_pid >= start_patid:
            chunks.append(chunk[chunk['ENC_NO'] != 0])
        else:
            chunks.append(chunk[(chunk['PAT_ID'] >= start_patid) &
                                (chunk['PAT_ID'] < end_patid) &
                                (chunk['ENC_NO'] != 0)])

    return pd.concat(chunks)


# def get_batched_results_dir(directory, start, end):
#     results_dir = os.path.join(directory,  os.path.join('results', 'ProgressNotes'))
#     batched_results_dir = os.path.join(results_dir, str(start)+'-'+str(end))
#     if not os.path.exists(batched_results_dir):
#         os.makedirs(batched_results_dir)
#     return batched_results_dir


@click.command()
@click.option('--start_range', '-s', default=1,
              help='Processing occurs in increasing order of PAT_ID. This is the starting number.')
@click.option('--end_range', '-e', default=1, help='End of the Pat-id folder range to process')
@click.option('--base_directory', '-b', help='Root/base directory to start processing from/drop results to.\n '
                                             'Expect: \'Patients\' and \'results\' directory subdirectories +'
                                             ' ProgressNoteIndex.csv and PatientMorbiditySorted.csv')
def main(start_range, end_range, base_directory):

    if not base_directory or not os.path.isdir(base_directory):
        click.help_option()
        raise click.BadOptionUsage(message="Passed an invalid value for base_directory")

    logger.debug('base dir : ' + base_directory)

    signal.signal(signal.SIGINT, signal_handler)

    prog_notes_file_index = os.path.join(base_directory, 'ProgressNotesIndex.csv')
    morbidity_csv = os.path.join(base_directory, 'PatientMorbiditySorted.csv')

    ids_dict = setup_fileindex_numbers(prog_notes_file_index, start_patid=start_range, end_patid=end_range)
    mdf = df_from_file(morbidity_csv, start_patid=start_range, end_patid=end_range)

    #batched_results_dir = get_batched_results_dir(base_directory, start_range, end_range)
    results_dir = os.path.join(base_directory,  os.path.join('results', 'ProgressNotes'))

    for prog_notes_filename, text in get_all_filename_text(ids_dict, mdf, results_dir, base_directory):
        with open(file=prog_notes_filename, mode='w') as wf:
            write_rtf(text, wf)

    log_results()

if __name__ == '__main__':
    main()


# PatientMorbiditySorted : Text column is constructed as follows :
# =IF(LEN(AR2&AQ2)=0,Y2,IF(LEN(AR2)>0,AR2,AQ2))
