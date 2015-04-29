# Script:current_data_to_netcdf.py
#        runs netcdf functions on current datafiles
# Modified by: Julia Signell
# Date created: 2015-04-16
# Date modified: 2015-04-29

import os
import sys
import re
import shutil
import datetime as dt
import zipfile
from posixpath import join

import split_big_files as split
import tower_to_netcdf as t2n

usr = 'Julia'
ROOTDIR = 'C:/Users/%s/Dropbox (PE)/KenyaLab/Data/Tower/' % usr
DATADIR = ROOTDIR + 'TowerData/'
TSDIR = DATADIR + 'CR3000_SN4709_ts_data/'
NETCDFDIR = DATADIR+'raw_netCDF_output/'
NETCDFPUB = DATADIR+'raw_netCDF_output/'


def fluxmain(input_dir, output_dir, attrs, old=False):
    # this function basically just gives DFList to t2n.process()
    print('running fluxmain with DATADIR = %s' % input_dir)
    DATAFILERE = re.compile('^CR\d{4}_SN\d+_(.*?)\.dat$')
    for f in os.listdir(input_dir):
        if DATAFILERE.match(f):
            print(f)
        else:
            continue
        file_to_netcdf(input_dir, output_dir, f, attrs, old=old)


def file_to_netcdf(input_dir, output_dir, f, attrs, old=False):
    input_file = join(input_dir, f)
    df = t2n.createDF(input_file)
    DFList = t2n.group_df_by_day(df, old=old)
    t2n.process(input_dir, input_file, output_dir, DFList, f, attrs, old=old)


def tsmain(input_dir, output_dir, attrs, old=False):
    print('running tsmain with TSDIR = %s' % input_dir)
    header_file = join(input_dir, 'header.txt')
    processed = open(join(input_dir, 'processed2netCDF.txt'), 'r').read()
    DFL = []
    for f in [f for f in os.listdir(input_dir) if 'ts_data' in f and f not in processed]:
        if '.dat' not in f:
            continue
        if 'zip' not in f:
            input_file = join(input_dir, f)
        else:
            archive = zipfile.ZipFile(join(input_dir, f), 'r')
            input_file = archive.extract(join('share', f.partition('.zip')[0]),
                                              output_dir)
        print('inport_file =', input_file)
        DFL.append(t2n.ts_run(input_dir, input_file, header_file, old=old))
        if len(DFL) > 1:
            DFList, DFL = split.group_by_day(DFL)
        else: 
            pass            
            #DFList = DFL[0]
        t2n.process(input_dir, input_file, output_dir, DFList, f, attrs,
                    header_file=header_file, old=old)
            

def main():
    # run functions for processing data files
    try:
        sys.stderr = open(NETCDFDIR + 'stderr.txt', 'a')
        sys.stdout = open(NETCDFDIR + 'stdout.txt', 'a')
    except:
        print('no stdout')

    fluxmain(DATADIR, NETCDFDIR, attrs, old=False)
    tsmain(TSDIR, NETCDFDIR, attrs, old=False)

    try:
        shutil.rmtree(join(NETCDFDIR, 'share'))
    except:
        pass

    t2n.createSummaryTable(NETCDFDIR)
    # zip_and_move(NETCDFLOC,NETCDFPUB)
    print('all done with the raw file production for today: %s!' %
          dt.datetime.today())

if __name__ == '__main__':
    main()
