'''
Create summary documents by reading file names in netcdf output dir.

Julia Signell
2015-05-11
'''

from __init__ import *


columns = ['filename']
paths = []
for path, dirs, files in os.walk(output_dir):
    for d in datafiles:
        if d in path.split('/'):
            columns.append(d)
            paths.append({'datafile': d, 'path': path, 'files': files})
todays_date = dt.datetime.utcnow().date()
first_date = dt.datetime(2010, 01, 01).date()
index = pd.date_range(start=first_date, end=todays_date,
                      name='date_time_UTC', freq='D')
df_ = pd.DataFrame(index=index, columns=columns)
for day in range(len(df_.index)):
    doy = df_.index[day].timetuple().tm_yday
    y = df_.index[day].year
    filename = 'raw_MpalaTower_%d_%03d.nc' % (y, doy)
    df_['filename'][df_.index[day]] = filename
    for d in paths:
        if filename in d['files']:
            statinfo = os.stat(posixpath.join(d['path'], filename))
            file_size = statinfo.st_size/1024
            # so we can tell when a file is partial
            df_[d['datafile']][df_.index[day]] = file_size
df_.to_csv(posixpath.join(output_dir, 'DataAvailability.csv'))

# make a figure so that we can visually see data gaps
ax = df_.plot(logy=True, figsize=(16, 8), linewidth=2, colormap='rainbow',
              title='Summary of Data Availability and Filesize')
ax.set_ylabel('Filesize [KB]')
plt.savefig(posixpath.join(output_dir, 'DataAvailability.jpg'))
print('Updated the Data Availability docs!')


