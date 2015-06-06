
# for a discussion on handing times in R:
# http://www.noamross.net/blog/2014/2/10/using-times-and-dates-in-r---presentation-code.html

require(ncdf)
require(chron)
require(lubridate)

ncfile = 'C:/Users/Julia/Documents/GitHub/MpalaTower/inspection/raw_netCDF_output/soil/raw_MpalaTower_2015_148.nc'
ds <- open.ncdf(ncfile)

# look what's in there...
ds

# Get data
ds.temp <- get.var.ncdf(ds,'Temp_05cm_Avg')
ds.t <- get.var.ncdf(ds,'time')

ds.tunits <- att.get.ncdf(ds, 'time', 'units')[['value']]
tustr <- strsplit(ds.tunits, " ")
unit <- unlist(tustr)[1]
if ':' not in ymd(unlist(tustr)[3]:
  origin = ymd(unlist(tustr)[3])
else:
  origin = ymd_hmd(unlist(tustr)[3])

date_times = origin+dminutes(ds.t)

ds.sites <- as.list(ds[['dim']][['site']][['vals']])
colnames(ds.temp) <- c(ds.sites)