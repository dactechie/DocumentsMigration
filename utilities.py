import os
import re
import csv
import shutil
import logging
import pandas as pd
import pyodbc
from sqlalchemy import create_engine
import urllib

logger = logging.getLogger(__name__)
filename_components_pattern = re.compile("\{([^}]+)\}")
POSSIBLE_EMPTY_FILESIZE_BYTES = 500


def get_filename_components(title_fstring):
    return filename_components_pattern.findall(title_fstring)


"""
    :returns None if there was an error finding the index file or it's format was messed up.
"""


def get_rows_after_header_check(header_checker_func, csv_file_path):
    try:
        with open(csv_file_path, 'r') as csv_file:
            reader = csv.reader(csv_file)
            if header_checker_func(next(reader)):
                for row in reader:  # todo check if num columns are same as header, also format
                    yield row
            else:
                logger.error(f'ERROR : header is not ok. {csv_file_path}')  # throw an exception here !
                return None
    except FileNotFoundError:
        logger.error(f'FileNoteFound {csv_file_path}')
        return None


def write_to_file(pid, src, dest, csv_file, src_size):
    csv_writer = csv.writer(csv_file)
    csv_writer.writerow([pid, src, dest, src_size])


def fn(arg):
    return True


def is_empty_with_whitespace(file):
    with open(file, mode='r') as datafile:
        non_whitespace_lines = [line for line in datafile if line.strip()]
        return not any(non_whitespace_lines)


def copyfile_with_checks(pid, src_file, destination_file, size):
    if os.path.isfile(destination_file):
        logging.info(f'{os.path.basename(destination_file)} exists. Skipping..')
        return
    if os.path.getsize(src_file) == 0:                      # todo we already have the size ( passed in as param)
        logging.warning(f'{pid} | 0kb:{src_file}. Skipping copy')
        return
    if int(size) < POSSIBLE_EMPTY_FILESIZE_BYTES and is_empty_with_whitespace(src_file):
        logging.warning(f'{pid} | {src_file} has only whitespaces. Skipping copy')
        return
    try:
        shutil.copyfile(src_file, destination_file)
    except OSError as oe:
        logger.exception(oe.filename + ' ' + str(oe.args))
        return None
    logger.info(f'{pid} | Copied {os.path.basename(src_file)} to {os.path.basename(destination_file)}')


def copy_using_output_csv(csv_file_path, src_path, dest_path, docprog_folder):
    rows = get_rows_after_header_check(fn, csv_file_path)
    src_template = f"{src_path}{os.sep}_pxd_{os.sep}{docprog_folder}{os.sep}_fxle_"
    for p, sfile, dfile, size in rows:
        src = src_template.replace('_pxd_', p).replace('_fxle_', sfile)
        copyfile_with_checks(p, src, os.path.join(dest_path, dfile), size)


def strip(text):
    try:
        return text.strip()
    except AttributeError:
        return text


server = r'DACT-52DT\SQLEXPRESS'
db = 'DIRECT_COPY' # 'Ccare'


def get_engine():
    params = urllib.parse.quote_plus(
        'DRIVER={SQL Server Native Client 11.0};' +  # 'ODBC Driver 17 for SQL Server' works too
        'SERVER=' + server + ';DATABASE=' + db + ';Trusted_Connection=yes')

    return create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
#
#
# def write_to_db_isok(df, pid):
#     #if not engine:
#     engine = get_engine()
#
#     # pd.set_option('display.max_columns', 20)
#     # pd.set_option('display.width', 200)
#
#     #write_data(engine, csvfile, table='DocumentIndex')
#
#     #df = pd.read_csv(csvfile)
#     #   print(df.head())
#     try:
#         df.to_sql('DocumentIndex', con=engine, if_exists='append', index=False)
#         return True
#     except pyodbc.DataError as de:
#         logger.error(f'pid:{pid} error: {de}')
#         return False
#     except exc.DataError as sqlal_de:
#         logger.error(f'pid:{pid} error: {sqlal_de}')
#         return False
#     except exc.DBAPIError as dbapie:
#         logger.error(f'pid:{pid} error: {dbapie}')
#         return False


def read_from_db():

    conn = pyodbc.connect('DRIVER={SQL Server Native Client 11.0};SERVER='
                          + server + ';DATABASE=' + db + ';Trusted_Connection=yes')

    # FILENAME  -> Alistair to add column ?
    # [ {'staff': 'PETA WRIGHT', 'timestamp': '31/10/2014 10:25:58 AM', 'fileno': '184', 'doctitle': 'Criminal History Zoran', 'ext': 'PDF ', 'pid': '6'}]
    sql = """
        select top 5 DOC_NO as fileno, CREATED_TIMESTAMP as timestamp, DOC_COMMENT, DOCUMENT_TYPE as ext
        from PAT_DOC
    """
    sql = """ select * from CCare.dbo.fn_GenerateDocumentNames (1, 100)"""

    # cursor = conn.cursor()
    # cursor.execute(sql)
    # for row in cursor.fetchall():
    #     print(row)

    pd.set_option('display.max_columns', 500)
    pd.set_option('display.width', 1000)

    df = pd.read_sql(sql, conn)

    print(df)

if __name__ =='__main__':
    read_from_db()

