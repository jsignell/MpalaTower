# Script:archive_data_to_netcdf.py
#        runs netcdf functions on old datafiles
# Modified by: Julia Signell
# Date created: 2015-04-16
# Date modified: 2015-04-29

import os
import shutil
import datetime as dt

from posixpath import join

import split_big_files as split
import tower_to_netcdf as t2n

usr = 'Julia'
ROOTDIR = 'C:/Users/%s/Dropbox (PE)/KenyaLab/Data/Tower/' % usr
ARCHIVEDIR = 'F:/'
DATADIR = ROOTDIR + 'TowerData/'
TSDIR = DATADIR + 'CR3000_SN4709_ts_data/'
NETCDFDIR = DATADIR+'raw_netCDF_output/'
NETCDFPUB = DATADIR+'raw_netCDF_output/'


def fluxmain(input_dir, output_dir, datas, old=True):
    # this is for processing files that are larger than 400MB. First split them
    # using the cygwin commands described and then run this program.
    print('running splitmain with input_dir = %s' % input_dir)
    little_files = split.split_big_files(input_dir, datas)
    try:
        processed = open(join(input_dir, 'processed2netCDF.txt'), 'r').read()
    except:
        processed = []
    for f in [f for f in little_files if f not in processed]:
        input_file = join(input_dir, f)
        DFList = t2n.createDF(input_file, old=old)
        t2n.process(input_dir, input_file, output_dir, DFList, f, old=old)
    DFL = []
    files = os.listdir(join(input_dir, 'split'))
    files.sort()
    for f in [f for f in files if f not in processed]:
        print('trying to run', f)
        input_file = join(input_dir, 'split', f)
        if f.endswith('00'):
            DFList = t2n.createDF(input_file, old=old)
            header_file = input_file
            DFL.append(DFList)
        else:
            DFList = t2n.createDF(input_file, header_file=header_file, old=old)
        DFL.append(DFList)
        if len(DFL) > 1:
            DFList, DFL = split.group_by_day(DFL)
            t2n.process(input_dir, input_file, output_dir, DFList, f,
                        header_file=header_file, old=old)
    try:
        shutil.rmtree(join(input_dir, 'split/'))
    except:
        pass


def tsmain(input_dir, output_dir, attrs, old=True):
    # DO NOT RUN THIS UNLESS YOU INTEND TO CLEAN UP
    header_file = join(input_dir, 'header.txt')
    #split.split_big_files(input_dir, ['ts_data'], header_file=header_file)
    DFL = []
    files = os.listdir(join(input_dir, 'split'))
    try:
        processed = open(join(input_dir, 'processed2netCDF.txt'), 'r').read()
    except:
        print('no processed file available')
        processed = ''
    for f in [f for f in files if f not in processed]:
        print('trying to run', f)
        input_file = join(input_dir, 'split', f)
        DFL.append(t2n.ts_run(input_dir, input_file, header_file, old=old))
        if len(DFL) > 1:
            DFList, DFL = split.group_by_day(DFL)
            t2n.process(input_dir, input_file, output_dir, DFList, f, attrs,
                        header_file=header_file, old=old)


def flux_archive_search(input_dir, output_dir, old=True):
    # steps roughly down through messy folders and finds interesting files
    datas = ['lws', 'licor', 'WVIA', 'flux',
             'upper', 'Manifold', 'Table1', 'Table1Rain']
    for path, dir, files in os.walk(input_dir):
        if any(data in file for file in files for data in datas):
            # checks if any data files are in a dir
            if 'split' not in path:
                path = path.replace('\\', '/')
                fluxmain(path, output_dir, datas, old=True)


def main():
    tsmain(TSDIR, NETCDFDIR, old=True)
    print('starting to process old files now: ', dt.datetime.now())
    keys = ['dvantech', 'ower', 'logger']
    dirs = []
    for dir in os.listdir(ARCHIVEDIR):
        if [key for key in keys if key in dir] != []:
            dirs.append(dir)
    for input_dir in dirs:
        try:
            flux_archive_search(join(ARCHIVEDIR, input_dir), NETCDFDIR,
                                old=True)
        except:
            print('can\'t handle %s' % input_dir)
        processed = open(join(ARCHIVEDIR, 'processed2netCDF.txt'), 'a')
        processed.write(input_dir + '\n')
        processed.close

    t2n.createSummaryTable(NETCDFDIR)

#if __name__ == '__main__':
#    main()
