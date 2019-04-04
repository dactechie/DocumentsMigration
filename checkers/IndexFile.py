import os
from abc import abstractmethod, ABC
from configparser import NoOptionError
import logging
import re
from datetime import datetime
from checkers.Staff import get_staff_logins
from utilities import get_filename_components, get_rows_after_header_check


logger = logging.getLogger(__name__)
filename_invalid_chars = re.compile('[<>:,_\/*?\"|]|\n')
filename_multi_dots = re.compile('[.]+')
EOL_WINDOWS = '\r\n'
STAFFNAME_MIGRATION = '2019migration'


class IndexFile(ABC):
    _config = None
    _title_idx = None
    _staff = None

    @classmethod
    def initialize(cls, config):
        cls._config = config
        cls._title_idx = cls._get_titleindices_from_config()

        # cols = config._sections[cls.__name__]['columns'].split(',')
        # login_fieldname = cols[int(config._sections[cls.__name__]['staff'])]

        return cls

    @classmethod
    def _get(cls, key):
        return cls._config.get(cls.__name__, key)

    @classmethod
    def isok_header(cls, row):
        return cls._get('columns') == ','.join(row)

    """
    for all the title items, get the indices for each of the columns in components destination filename.
    """

    @classmethod
    def _get_titleindices_from_config(cls):
        index_dict = {}
        for tic in get_filename_components(cls._get('titlefstring')):
            try:
                index_dict[tic] = int(cls._get(tic))  # file_no=0  timestamp=1
            except NoOptionError:                     # e.g. pid
                logger.debug(f'No {tic} in title_items. (not expecting to have a pid)')
                continue
        return index_dict

    @classmethod
    def _get_data(cls, row, col_name, col_idx):
        value = row[col_idx]
        if not value:
            alt_col_name = f'alt{col_name}'
            value = row[int(cls._get(alt_col_name))]
            logger.debug(f'alt column {alt_col_name} value : {value}. going to sanitize..')
            value = getattr(cls, f'fix_{alt_col_name}')(value)
            # sanitize here as, only the titlefstring components will be sanitized later.
        return value

    @classmethod
    @abstractmethod
    def build_src_filenames(cls, idx_data):
        pass

    """
        title-items :  (pid, staff, ...) from : title=pid__staff__timestamp__+filename__.__ext__altfilename
        title_items_conf : a list of title items
        title_indices : dict with indices of title-items that have indices : staff, timestamp, etc. 
                            (doesn't contain : pid, +filename)
        Note: not a generator because, unless all rows succeed, we dont want to do anything with this pat-id
        :returns None if there is an issue with the index rows or if unable to form a file extension.
    """

    @classmethod
    def get_idxfile_data(cls, pid, index_file_path):
        item_dicts = []
        built_src_filenames = []
        for row in get_rows_after_header_check(cls.isok_header, index_file_path):
            if not row:
                logger.warning(f'{pid} | Index file has blank row {index_file_path}!')
                return None
            item_dict = {}
            try:
                for col in cls._title_idx:
                    item_dict[col] = cls._get_data(row, col, cls._title_idx[col])
            except NoOptionError:
                logger.error(f'{pid} | Index file has Missing Value for field: {col}. Skipping pid. {index_file_path}')
                return None, None

            item_dict['pid'] = pid
            built_src_filenames.append(cls.filename_from_index(item_dict))
            item_dicts.append(item_dict)

        return item_dicts, built_src_filenames

    @classmethod
    @abstractmethod
    # @value_check
    def filename_from_index(cls, idx_dict) -> str:
        return ""

    @classmethod
    def get_destination_filename(cls, idxfile_row):
        clean = {item: getattr(cls, f'fix_{item}')(idxfile_row[item]) for item in idxfile_row}
        if not clean:
            return None

        result = cls._get('titlefstring').format(**clean)

        result = filename_multi_dots.sub('.', result)
        logger.debug(result)
        return result

    # @classmethod
    # def value_check(cls, func_to_check):
    #     def wrapper_function(self, *args):
    #         if args[0]:
    #             try:
    #                 return func_to_check(self, args[0])
    #             except ValueError:
    #                 logger.warning(f'ValueError for {func_to_check.__name__} args: {str(args[0])}')
    #                 return None
    #         else:
    #             logger.error(f'None was passed to {func_to_check.__name__}')
    #             return None
    #
    #     return wrapper_function

    @classmethod
    # @value_check
    def fix_fileno(cls, v):
        int(v)
        return v

    @classmethod
    # @value_check
    def fix_pid(cls, v):
        # if not represents_int(v):
        #     return None
        int(v)
        return v

    @classmethod
    # @value_check
    def fix_timestamp(cls, timestamp):
        dt = datetime.strptime(timestamp, '%d/%m/%Y %I:%M:%S %p')
        return str(dt).replace(':', '.')

    # @classmethod
    # @abstractmethod
    # # @value_check
    # def fix_doctitle(cls, title):
    #     pass

    """
        fix_altdoctitle: Cleans-up the filename text
        removes the extension if it is part of the FILENAME field text. the actual extension of the file is different
        from the contents of the FILENAME field.

    # not calling fix_doctitle here as it will be called when fixing the title during get_destination_filename.
    # bad practice to have this expectation ?
    """

    @classmethod
    # @value_check
    def fix_ext(cls, v):
        ext = v.strip().lower()
        lext = len(ext)
        return None if lext < 3 or lext > 4 else ext

    @classmethod
    @abstractmethod
    # @value_check
    def fix_staff(cls, staff_name):
        return cls._staff.get(staff_name, None)


class ProgressNoteIndex(IndexFile):

    @classmethod
    def initialize(cls, config):
        cls = super().initialize(config)
        cls._staff = get_staff_logins('login')
        return cls

    @classmethod
    def build_src_filenames(cls, idx_data):
        return [f"{item_dict['fileno']}.rtf" for item_dict in idx_data]

    @classmethod
    # @IndexFile.value_check
    def fix_staff(cls, staff_name):
        staff_name = staff_name.strip().lower()
        staff_login = super().fix_staff(staff_name)
        if staff_login:
            return staff_login

        migr_staff = f'{staff_name}.{STAFFNAME_MIGRATION}'
        if migr_staff in cls._staff:
            return cls._staff[migr_staff]
        else:
            logger.error(f'Skipping invalid staff name {staff_name}')
            return None

    @classmethod
    def filename_from_index(cls, idx_dict):
        return f"{idx_dict['fileno']}.rtf"


class DocumentIndex(IndexFile):

    @classmethod
    def initialize(cls, config):
        cls = super().initialize(config)
        cls._staff = get_staff_logins('display_login')
        return cls

    @classmethod
    def build_src_filenames(cls, idx_data):
        src_files = []
        for item_dict in idx_data:
            # src files checker-helper dict. A bit hacky; needs a more soph .ini parser
            ext = cls.fix_ext(item_dict['ext'])
            if ext:
                src_files.append(f"{item_dict['fileno']}.{ext}")
            else:
                logger.error(f"{pid} | Unable to form .ext for file {cls._get('fileno')}")
                return None
        return src_files

    @classmethod
    # @value_check
    def fix_doctitle(cls, title):
        if not isinstance(title, str):
            logger.debug("Empty title. skipping..")
            return None

        return filename_invalid_chars.sub('.', title).strip()

    @classmethod
    # @value_check
    def fix_altdoctitle(cls, title):
        if '.' not in title:
            logger.error(f'No Extension in altdoctitle : {title}')
            return None
        file_title, extension = os.path.splitext(title)
        if extension and extension in ['.rtf', '.pdf', '.jpg', '.png', '.doc', '.txt']:
            title = file_title
        return title

    @classmethod
    # @IndexFile.value_check
    def fix_staff(cls, staff_name):
        staff_name = staff_name.strip().lower()
        staff_login = super().fix_staff(staff_name)
        if staff_login:
            return staff_login

        migr_staff = f'{staff_name}.{STAFFNAME_MIGRATION}'
        if migr_staff in cls._staff:
            return cls._staff[migr_staff]
        else:
            logger.error(f'Skipping invalid staff name {staff_name}')
            return None

    @classmethod
    def filename_from_index(cls, idx_dict):
        return f"{idx_dict['fileno']}.{idx_dict['ext'].strip().lower()}"
