# This file is meant as a rough guide to opening and using netcdf files in 
# matlab or the open source version, Octave.

# for more uptodate info check out:
# http://www.mathworks.com/help/matlab/network-common-data-form.html

import_netcdf

# give the path or OpenDAP url for the file
ncfile = 'C:/Users/Julia/Documents/GitHub/MpalaTower/inspection/raw_netCDF_output/
soil/raw_MpalaTower_2015_145.nc';

# check what is inside the file
ncdisp(ncfile);

# get the units for time, so that you understand what it means
timeunits = ncreadatt(ncfile,'time','units');
disp(timeunits);

# get all the data for one variable without opening the file all the way
temp = ncread(ncfile,'Temp_05cm_Avg');
time = ncread(ncfile,'time');

# open the file in matlab
ncid = netcdf.open(ncfile, 'NC_NOWRITE');

# check out how many dimensions, variables, and attributes it contains
[ndims,nvars,natts] = netcdf.inq(ncid);

# get the name and type of the first dimension (should be site):
[dimname,dimlen] = netcdf.inqDim(ncid,0)

# get the name, type of variable, dimension ids and number of attributes for 
# the first variable
[varname,vartype,dimids,natts] = netcdf.inqVar(ncid,0)

# get all the values for a particular variable
A_number = netcdf.getVar(ncid,0)

# close the file
netcdf.close(ncid)
