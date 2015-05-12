MpalaTower
==========
We are in the process of developing a system for getting tower data online and accessible. The hope is that this will be transferable to other timeseries data. What we have are a collection of asci .dat datafiles spanning 5 years. What we want is a queryable dataset that can be quickly inspected graphically. 


###Data munging
We have a developed a set of python scripts to convert the large .dat files to daily netCDF files. Since the data are collected at different frequencies, it does not make sense to merge across datafiles. All of the files that relate to this conversion are in the munging dir.

#####TO DO:
- [x] fix time in scripts to run faster and work with ts_data
- [x] clean up and speed up tower\_to\_netcdf
- [ ] add functionality to glean units from parameter names

###Data overview
We have created an app for viewing a summary of daily data from the tower. The files for this app are stored in the repo: https://github.com/kcaylor/tower_metadata/ and the app itself is at http://mpala.herokuapp.com/

#####TO DO:
- [ ] see what else can be gleaned from program files
- [ ] add to program files so that instruments can be pulled in
- [ ] improve unit-based flags
- [ ] make a download button that leads to a Dropbox URL

###More indepth
We are working on developing a tool or set of tools for doing more indepth analysis of the tower data. These tools are stored in the inspection dir

#####TO DO:
- [ ] inspect netCDF files
- [ ] clean and merge netCDF files over selected date range
- [ ] plot netCDF files
