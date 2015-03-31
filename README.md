MpalaTower
==========
We are in the process of developing a system for getting tower data online and accessible. The hope is that this will be transferable to other timeseries data. What we have are a collection of txt datafiles spanning 5 years. What we want is a queryable dataset that can be quickly inspected graphically. 
To do this we convert the large txt files to daily netcdf files. Since the data are collected at different frequencies, it does not make sense to merge across datafiles.

##Done
======
We have a script that converts between all of the .dat files and netcdf. 

##TO DO
=======
- [ ] inspect recent netcdf files
- [ ] clean and merge recent netcdf files
- [ ] plot recent netcdf files
- [ ] allow an option for selecting date range
- [ ] get it online
- [ ] fill in historic data gaps with files from external hard drives (if possible)
