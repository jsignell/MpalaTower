# Script:split_big_files.py
#        splits big data files from mpala tower so that the script can run
#        without crashing the computer
# Modified by: Julia Signell
# Date created: 2015-04-09
# Date modified: 2015-04-16

import os
import pandas as pd
from posixpath import join


def split_big_files(input_dir, datafiles, header_file=None):
    # BEWARE THAT THERE ARE BACKUP FILES WITH THE FIRST 3000 LINES BELONGING AT
    # THE END. YOU NEED TO CORRECT FOR THIS USING CYGWIN COMMANDS
    k = 0
    little_files = []
    if not os.path.exists(join(input_dir, 'split')):
        os.mkdir(join(input_dir, 'split'))
    for f in os.listdir(input_dir):  # a directory in which there are datafiles
        conditions = ('.zip' not in f, 'ts_data' not in f, f.startswith('.'))
        if conditions is (True, True, False):
            pass
        else:
            continue
        if header_file is None:
            header_file = join(input_dir, f)
        for datafile in datafiles:
            if datafile in f:
                datafile = datafile
            else:
                continue
        print(f)
        statinfo = os.stat(join(input_dir, f))
        file_size = statinfo.st_size
        if file_size >= 200000000:
            print(file_size)
            in_out_f = ('%s_%02d' % (join(input_dir, f) +
                        ' ' + join(input_dir, 'split', datafile), k))
            os.system(r'C:\cygwin\bin\bash.exe --login -c "split -C 180M -d %s"' %
                      in_out_f)
            print('processed %s' % in_out_f)
            k += 1
        else:
            little_files.append(f)
    print(little_files)
    return little_files


def group_by_day(DFL):
    # groups by day across split datafiles
    DFList = []
    if len(DFL[0]) > 1:
        for df in DFL[0][0:-1]:
            DFList.append(df)
    if DFL[0][-1].index[-1].date() == DFL[1][0].index[0].date():
        df = pd.concat([DFL[0][-1], DFL[1][0]])
        DFL[1].pop(0)
    else:
        df = DFL[0][-1]
    DFList.append(df)
    DFL.pop(0)
    print('DFL has length %s' % len(DFL))
    print('DFL[0] has length %s' % len(DFL[0]))
    if len(DFL[0]) == 0:
        DFL = []
    return DFList, DFL
