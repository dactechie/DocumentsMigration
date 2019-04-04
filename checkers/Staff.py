import csv


def get_staff_logins(staff_field):
    with open('staff.csv', 'r') as file:
        csv_reader = csv.reader(file)
        next(csv_reader)  # skip header row

        if staff_field == 'login':  # PROGRESS_NOTES CREATED_USER
            staff = {rows[0].lower().replace('\'', ''): rows[0] for rows in csv_reader}
            staff['hossein.tabatabaeijafari'] = 'Hossein.Tabatabaei-Jafari'
            staff['tilly.diebert'] = 'Tilly.Gardner'
            staff['tammy.waters'] = 'Tamara.Waters'
        else:               # DOCUMENT : RECEIVING_DR_NAME
            staff = {rows[2].lower(): rows[0] for rows in csv_reader} # display name

        return staff

# def initialize_staff():
#     _staff = get_staff_logins()
#
#     def get_staff():
#         return _staff
#     return get_staff



# def get_staff_ids():
#     with open('mcare-staff-ids.csv', 'r') as file:
#         csv_reader = csv.reader(file)
#         next(csv_reader)  # skip header row
#         return {rows[1]: rows[0] for rows in csv_reader}


