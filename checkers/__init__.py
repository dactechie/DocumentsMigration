# import os
# from datetime import datetime
# import logging
#
# log_file_name = f'logs\\debug_{datetime.now().strftime("%Y%m%d_%H%M%S")}-{os.path.basename(__file__)[:-3]}.log'
# logging.basicConfig(filename=log_file_name,
#                     level=logging.DEBUG,
#                     format='%(levelname)s - %(message)s')    # - %(name)s - %(threadName)s
import os
import json
import logging.config

from .IndexFile import IndexFile
# from .Staff import Staff


default_path = 'config\\logging.json'
default_level = logging.WARN
env_key = 'LOG_CFG'

path = default_path
value = os.getenv(env_key, None)
if value:
    path = value
if os.path.exists(path):
    with open(path, 'rt') as f:
        config = json.load(f)
    print("***************using config file for setting up logger")
    logging.config.dictConfig(config)
else:
    logging.basicConfig(level=default_level,
                        filename=default_path,
                        format="%(asctime)s - %(levelname)s - %(message)s")

# Log that the logger was configured
logger = logging.getLogger(__name__)
#logger.info('Completed configuring logger()!')