import os
import logging

logger = logging.getLogger(__name__)


def validate(pid, built_filenames, pid_folder_root, log, ignore_file):

    filenames = [f for f in os.listdir(pid_folder_root) if os.path.basename(f) != ignore_file]

    notin_indexbuilt_files = [f for f in filenames if f not in built_filenames]
    notin_folderfiles = [f for f in built_filenames if f.lower().strip() not in filenames]

    if notin_indexbuilt_files:
        logger.error(f'{pid}{log} | not in index files: {str(notin_indexbuilt_files)}')
    if notin_folderfiles:
        logger.error(f'{pid}{log} | in index file not in folder: {str(notin_folderfiles)}')

    return notin_indexbuilt_files, notin_folderfiles
