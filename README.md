MpalaTower
==========
We are in the process of developing a system for getting tower data online and accessible. The hope is that this will be transferable to other timeseries data. What we have are a collection of txt datafiles spanning 5 years. What we want is a queryable dataset that can be quickly inspected graphically. 


###Data munging
We have a developed a set of python scripts to convert the large txt files to daily netcdf files. Since the data are collected at different frequencies, it does not make sense to merge across datafiles. All of the files that relate to this conversion are in the data_munging dir.

#####TO DO:
- [ ] fix time in scripts to run faster and work with ts_data
- [ ] clean up and speed up tower\_to\_netcdf

###Data overview
We have created an app for viewing a summary of daily data from the tower. The files for this app are stored in the repo: https://github.com/kcaylor/tower_metadata/ and the app itself is at http://mpala.herokuapp.com/

#####TO DO:
- [ ] see what else can be gleaned from program files
- [ ] add to program files so that instruments can be pulled in
- [ ] improve unit-based flags
- [ ] make a download button that leads to a dropbox URL

###More indepth
We are working on developing a tool or set of tools for doing more indepth analysis of the tower data. These tools are stored in the analysis dir

#####TO DO:
- [ ] inspect netcdf files
- [ ] clean and merge netcdf files
- [ ] plot netcdf files
- [ ] allow an option for selecting date range
- [ ] fill in historic data gaps with files from external hard drives (if possible)
