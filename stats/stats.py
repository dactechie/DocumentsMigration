import os
import pandas as pd

##
#  show errors
#
#
#



filename = os.path.join(r"..\logs", 'info.log')

df = pd.read_csv(filename
                 , sep='|'
                 , skiprows=1
                 , skip_blank_lines=True
                 , header=None
                 , names=['day_time', 'pid_ftype', 'src_dest']
                 , engine='python')


starts_with_src = df[df['src_dest'].str.startswith(" src:")]


print(starts_with_src)