'''
Process datafiles from input_dicts and write daily netcdf files.

Julia Signell
2015-05-07
'''

from __init__ import *
from find_files import get_files
import TOA5_to_netcdf as t2n


def run(DFList, input_dict, output_dir, attrs, coords, **kwargs):
    '''Process .dat file and write daily netcdf files.'''
    if kwargs.get('archive') is False and kwargs.get('allow_partial') is False:
        DFList = DFList.pop(-1)  # cut off last day

    ncfilenames = t2n.get_ncnames(DFList)
    out_path = posixpath.join(output_dir, input_dict['datafile'])
    attrs, local_attrs = t2n.get_attrs(input_dict['header_file'], attrs)
    for i in range(len(DFList)):
        df = DFList[i]
        nc = ncfilenames[i]
        if nc not in os.listdir(out_path) or kwargs.get('rerun') is True:
            ds = t2n.createDS(df, input_dict, attrs, coords, local_attrs)
            output_path = posixpath.join(out_path, nc)
        elif kwargs.get('allow_partial') is True:
            if i == 0 or i == len(DFList)-1:
                ds = t2n.createDS(df, input_dict, attrs, coords, local_attrs)
                output_path = posixpath.join(out_path, nc)
                ds_temp = xray.open_dataset(output_path)
                xray.concat(ds_temp, ds, dim= 'time', mode='different')
            else:
                continue
        else:
            continue
        ds.to_netcdf(path=output_path, mode='w', format='NETCDF3_64BIT')

    if kwargs.get('archive') is True:
        processed_file = posixpath.join(input_dict['path'],
                                        'processed2netCDF.txt')
        processed = open(processed_file, 'a')
        processed.write(input_dict['filename']+'\n')


def run_splits(input_dict, output_dir, attrs, coords, **kwargs):
    '''Run split files so that they append for each day.'''
    DFL = []
    for f in input_dict['splits']:
        input_file = posixpath.join(input_dict['path'], 'split', f)
        if f.endswith('00'):
            pass
        else:
            input_dict.pop('has_header')
            input_dict.update({'has_header': False})
        DFList = t2n.createDF(input_file, input_dict)
        DFL.append(DFList)
        if len(DFL) > 1:
            DFList, DFL = split.group_by_day(DFL)
            run(DFList, input_dict, output_dir, attrs, coords, **kwargs)
    try:
        shutil.rmtree(path_join(input_dir, 'split/'))
    except:
        pass


def run_wholes(input_dict, output_dir, attrs, coords, **kwargs):
    '''Run whole files'''
    input_file = posixpath.join(input_dict['path'], input_dict['filename'])
    DFList = t2n.createDF(input_file, input_dict)
    run(DFList, input_dict, output_dir, attrs, coords, **kwargs)


def just_do_it(input_dir, output_dir, attrs, coords, **kwargs):
    input_dicts = get_files(input_dir, **kwargs)
    for input_dict in input_dicts:
        if 'splits' in input_dict.keys():
            run_splits(input_dict, output_dir, attrs, coords, **kwargs)
        else:
            run_wholes(input_dict, output_dir, attrs, coords, **kwargs)
