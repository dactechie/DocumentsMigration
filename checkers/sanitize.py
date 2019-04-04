# import os
# from datetime import datetime
# from checkers.Staff import initialize_staff
# import logging
# import re
#
# filename_invalid_chars = re.compile('[<>:,_\/*?\"|]')
#
# logger = logging.getLogger(__name__)
#
#
#



# for progress notes
# @value_check
# def fix_staff(staff_name):
#     staff_name = staff_name.strip().lower() #.title()
#     staff_dict = initialize_staff()
#     logins = staff_dict['login']
#     if staff_name in logins:
#         return logins[staff_name]
#     elif staff_name + '.2018migration' in logins:
#         return logins[staff_name + '.2018migration']
#     else:
#         logger.error(f'Skipping invalid staff name {staff_name}')
#         return None

# @value_check
# def fix_staff(staff_name):
#
#     staff_name = staff_name.strip().title()
#     staff_dict = staff()
#     logins = staff_dict['login']
#     disp_login = staff_dict['displayname']
#     result = None
#     try:
#         if staff_name in logins:
#             return logins[staff_name]
#         elif staff_name in disp_login:
#         result = logins.get(
#             staff_name,
#             disp_login.get(
#                 staff_name,
#                 disp_login.get(
#                     staff_name + ' 2018Migration',
#                     logins.get(
#                         staff_name + '.2018Migration',
#                         staff_dict['displaynames_lower'][staff_name.lower()]
#                     )
#                 )
#             )
#         )
#     except KeyError:
#         logger.error(f'Skipping invalid staff name {staff_name}')
#
#     return result


"""
    fix_fileno and fix_pid  : If casting their argument fails, then they don't represent ints
"""
#
#
# @value_check
# def fix_fileno(v):
#     int(v)
#     return v
#
#
# @value_check
# def fix_pid(v):
#     # if not represents_int(v):
#     #     return None
#     int(v)
#     return v
#
#
# @value_check
# def fix_timestamp(timestamp):
#     dt = datetime.strptime(timestamp, '%d/%m/%Y %I:%M:%S %p')
#     return str(dt).replace(':', '.')
#
#
# @value_check
# def fix_doctitle(title):
#     return filename_invalid_chars.sub('.', title)
#
#
# """
#     fix_altdoctitle: Cleans-up the filename text
#     removes the extension if it is part of the FILENAME field text. the actual extension of the file is different
#     from the contents of the FILENAME field.
#
# # not calling fix_doctitle here as it will be called when fixing the title during get_destination_filename.
# # bad practice to have this expectation ?
# """
#
#
# @value_check
# def fix_altdoctitle(title):
#     if '.' not in title:
#         logger.error(f'No Extension in altdoctitle : {title}')
#         return None
#     file_title, extension = os.path.splitext(title)
#     if extension and extension in ['.rtf', '.pdf', '.jpg', '.png', '.doc', '.txt']:
#         title = file_title
#     return title
#
#
# @value_check
# def fix_ext(v):
#     ext = v.strip().lower()
#     lext = len(ext)
#     return None if lext < 3 or lext > 4 else ext
#
#
