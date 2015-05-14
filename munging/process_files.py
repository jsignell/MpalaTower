'''
Process datafiles from input_dicts and write daily netcdf files.

Julia Signell
2015-05-07
'''

from __init__ import *
import TOA5_to_netcdf as t2n


def done_processing(input_dict, **kwargs):
    if 'ts_data' in input_dict['filename']:
        os.remove(posixpath.join(input_dict['path'], input_dict['filename']))
        start = ''
        for i in input_dict['path'].split('/')[0:-1]:
            start = posixpath.join(start, i)
        processed_file = posixpath.join(start, 'processed2netCDF.txt')

    elif kwargs.get('archive') is True:
        processed_file = posixpath.join(input_dict['path'],
                                        'processed2netCDF.txt')
    else:
        return
    processed = open(processed_file, 'a')
    processed.write(input_dict['filename']+'\n')
    return


def merge_partials(attrs, df, output_path):
    print(output_path)
    ds = xray.open_dataset(output_path)
    if ds.attrs['program'] != attrs['program']:
        return df
    df_old = ds.to_dataframe()
    new_index = pd.DatetimeIndex(((df_old.index.asi8/(1e8)).round()*1e8).astype(np.int64)).values
    df_old = df_old.set_index(new_index)
    df_old.index.name = 'time'
    for col in df_old.columns:
        if col not in df.columns:
            df_old.pop(col)
    df = df_old.append(df)
    df.sort(axis=1, inplace=True)
    df = df.drop_duplicates()
    return df


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
        output_path = posixpath.join(out_path, nc)
        if nc not in os.listdir(out_path):
            pass
        elif i == 0 or i == len(DFList)-1:
            df = merge_partials(attrs, df, output_path)
        elif kwargs.get('rerun') is True:
            pass
        else:
            continue
        ds = t2n.createDS(df, input_dict, attrs, coords, local_attrs)
        ds.to_netcdf(path=output_path, mode='w', format='NETCDF3_64BIT')
    done_processing(input_dict, **kwargs)


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
        DFList = t2n.createDF(input_file, input_dict, attrs)
        DFL.append(DFList)
        if len(DFL) > 1:
            DFList, DFL = t2n.group_by_day(DFL)
            run(DFList, input_dict, output_dir, attrs, coords, **kwargs)
    try:
        shutil.rmtree(path_join(input_dir, 'split/'))
    except:
        pass


def run_wholes(input_dict, output_dir, attrs, coords, **kwargs):
    '''Run whole files'''
    input_file = posixpath.join(input_dict['path'], input_dict['filename'])
    DFList = t2n.createDF(input_file, input_dict, attrs)
    run(DFList, input_dict, output_dir, attrs, coords, **kwargs)
