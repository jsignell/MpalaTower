'''
Find files of interest and rangle them into an input_dict

Julia Signell
2015-05-07
'''

from __init__ import *


def already_processed(path, f):
    '''Return True if a file has already been processed to netcdf.'''
    try:
        processed_file = posixpath.join(path, 'processed2netCDF.txt')
        processed = open(processed_file, 'r').read()
    except:
        processed = []
    if f in processed:
        return True
    else:
        return False


def fresh_processed(path):
    processed_file = posixpath.join(path, 'processed2netCDF.txt')
    if os.path.isfile(processed_file):
        processed = open(processed_file, 'w')
        processed.write('')
        processed.close


def has_header(input_dict):
    '''Update input_dict to reflect whether or not a file has a header.'''
    input_file = posixpath.join(input_dict['path'], input_dict['filename'])
    header = list(pd.read_csv(input_file, nrows=1).columns.values)
    if header[0] == 'TOA5':
        return input_dict.update({'has_header': True,
                                  'header_file': input_file})
    else:
        try:
            header_file = posixpath.join(input_dict['path'], 'header.txt')
        except:
            if input_dict['datafile'] == 'ts_data':
                header_file = posixpath.join(TSDIR, 'header.txt')
            else:
                print('couldn\'t find header file')
                header_file = None
        return input_dict.update({'has_header': False,
                                  'header_file': header_file})


def split_file(k, input_dict):
    d = input_dict['datafile']
    f = input_dict['filename']
    path = input_dict['path']
    split_path = posixpath.join(path, 'split')
    if not os.path.exists(split_path):
        os.mkdir(split_path)
    print(file_size)
    in_out_f = (posixpath.join(path, f) + ' ' +
                '%s_%02d' % (posixpath.join(split_path, d), k))
    os.system(r'C:\cygwin\bin\bash.exe --login -c "split -C 180M -d %s"' %
              in_out_f)
    print('processed %s' % in_out_f)
    splits = [x for x in os.listdir(split_path) if x.startswith('%s_%02d' %
                                                                (d, k))]
    input_dict.update(dict(splits=splits))
    k += 1
    return k, input_dict


def get_files(input_dir, archive=False, rerun=True, allow_partial=False):
    '''Return list of data dicts containing path, filename, datafile'''
    input_dicts = []
    for path, dirs, files in os.walk(input_dir):
        k = 0
        path = path.replace('\\', '/')
        print(path)
        if rerun is True:
            fresh_processed(path)
        for f in files:
            exclude = ('.zip' in f, f.startswith('.'))
            if exclude != (False, False):
                continue
            if archive is True and rerun is False:
                if already_processed(path, f) is True:
                    continue
            for d in datafiles:
                if d in f:
                    input_dict = ({'path': path,
                                   'filename': f,
                                   'datafile': d})
                    has_header(input_dict)
                    big = 200000000
                    if os.stat(posixpath.join(path, f)).st_size >= big:
                        k, input_dict = split_file(k, input_dict)

                    input_dicts.append(input_dict)
    return input_dicts
