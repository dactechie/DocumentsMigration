# result = "sad..f....aSD"
# print (result.replace('..', '.'))
#
# import re
#
#
#
# filename_invalid_chars = re.compile('[<>:,_\/*?\"|]|\n')
# filename_multi_dots = re.compile('[.]+')
#
# result = "sa<:d..f...\n" \
#          ".aSD"
# #result = re.sub('[.]+', '.', result)
#
# result = filename_invalid_chars.sub('.', result)
# print(filename_multi_dots.sub('.', result))



import re

from abc import abstractmethod, ABC
class IxFile(ABC):

    @classmethod
    @abstractmethod
    def fix_doctitle(cls, title):
        pass


class DocumentIxFile(IxFile):

    @classmethod
    def fix_doctitle(cls, title):
        print(f"title is Document Title {title}")

    @classmethod
    def fix_altdoctitle(cls, title):
        print(f"ALTT title is Document {title}")


class ProgressNoteIxFile(IxFile):

    @classmethod
    def fix_doctitle(cls, title):
        print(f"title is  ProgNote {title}")


DocumentIxFile.fix_doctitle("doc tuitle")
DocumentIxFile.fix_altdoctitle("alt doc title")
ProgressNoteIxFile.fix_doctitle("sad")
ProgressNoteIxFile.fix_altdoctitle("alt doc title")