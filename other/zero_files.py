import os

#
# WARNING :  not using absolute path. assumes program is run from same folder as src, dest root
#

# Global "constants"
base_dir = ""#""S:\Directions Central\Mastercare Development\MastercareImplementation\Migration in progress data\Migration Patient Documents"
main_dir_name = os.path.join(base_dir, 'Patients')
results_dir = os.path.join(base_dir, 'results')

DOCUMENTS = 1
PROGRESS_NOTES = 2

DOC_TYPE = {
    DOCUMENTS: {
        'folder': 'Documents',
        'index_file': 'DocumentIndex.csv'
    },
    PROGRESS_NOTES: {
        'folder': 'ProgressNotes',
        'index_file': 'ProgressNoteIndex.csv'
    }
}


def calculate_zero_nums(pat_ids, file_type=DOCUMENTS):
    str_pat_folder_names = [os.path.join(main_dir_name, os.path.join(str(pat_id), DOC_TYPE[file_type]['folder']))
                            for pat_id in pat_ids]  # Patients/*/Documents

    folders_with_data_files = filter(lambda a: os.path.isdir(a), str_pat_folder_names) # is there a patient folder with this id

    ignore_file = DOC_TYPE[file_type]['index_file']

    zeros_count = 0
    total_files = 0

    for pat_doc_folder in folders_with_data_files: #loop through all Patients/*/Documents

        files = [f for f in os.listdir(pat_doc_folder) if os.path.basename(f) != ignore_file]

        num_files = len(files)
        if num_files < 1:
            print(' .... skipping empty folder: {} ... '.format(pat_doc_folder))
            continue

        src_list = list(map(
            lambda f: os.path.join(pat_doc_folder, f)
            , files))
        zeros_count += [os.path.getsize(f) for f in src_list].count(0)
        total_files = total_files + num_files

    print(' empty files  {}   non zeros :{}'.format(str(zeros_count), str(total_files)))


def main():
    calculate_zero_nums(range(1, 10), PROGRESS_NOTES)


if __name__ == "__main__":
    main()
