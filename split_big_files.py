#Script:split_big_files.py
#       Splits big data files from mpala tower so that the script can run with
#       out crashing the computer
#Modified by: Julia Signell
#Date created: 2014-04-09

import os
import pandas as pd
import tower_to_netcdf as t2n

usr = 'Julia'
ROOTDIR = 'C:/Users/%s/Dropbox (PE)/KenyaLab/Data/Tower/'%usr
E = 'E:/'
ARCHIVEDIR = E + 'TowerDataArchive/BigFiles/'
DATALOC = ROOTDIR + 'TowerData/'
TSLOC = E+'TowerDataArchive/towerraw/ts_data/'
SPLITLOC = 'C:/cygwin/home/Julia/BigFiles/'
NETCDFLOC = DATALOC+'raw_netCDF_output/'
NETCDFPUB = DATALOC+'raw_netCDF_output/'

# To run the program foo that takes an option and two arguments
# Equivalent to "foo -d bar baz" directly in the shell
#call(['split', '-d', 'bar', 'baz'])

            #n = f.split('_')
            #split_file = 'split_'+n[2]+'_'+n[3]+'_'+n[4]+'_'+n[5]

def split_big_files(TSLOC):
    #BEWARE THAT THERE ARE BACKUP FILES WITH THE FIRST 3000 LINES BELONGING AT
    #THE END. YOU NEED TO CORRECT FOR THIS USING CYGWIN COMMANDS
    k=0
    for f in os.listdir(TSLOC):
        print(f)
        statinfo = os.stat(os.path.join(TSLOC,f))
        file_size = statinfo.st_size 
        if file_size >= 250000000:
            print(file_size)
            print(TSLOC+f, TSLOC+'ts_data_%s_'%str(k))
            os.system(r'C:\cygwin\bin\bash.exe --login -c "split -C 220M -d %sts_data_%s"'%
                      (TSLOC+f+' '+TSLOC+'split/',str(k)))            
            k+=1

def group_by_day(DFL):
    #groups by day across split datafiles    
    DFList = []
    if len(DFL[0])>1:
        for df in DFL[0][0:-1]:
            DFList.append(df)
    if DFL[0][-1].index[-1].date()==DFL[1][0].index[0].date():
        df = pd.concat([DFL[0][-1],DFL[1][0]])
        DFL[1].pop(0)
    else: df = DFL[0][-1]
    DFList.append(df)
    DFL.pop(0)
    print('DFL has length %s'%len(DFL))
    print('DFL[0] has length %s'%len(DFL[0]))
    if len(DFL[0])==0:
        DFL = []
    return DFList,DFL
    
def flux_split_main(SPLITLOC,NETCDFLOC,old=True):
    #this is for processing files that are larger than 400MB. First split them
    #using the cygwin commands described and then run this program.
    print 'running splitmain with SPLITLOC = %s'%SPLITLOC
    for f in os.listdir(SPLITLOC):
        if 'split' in f:
            n = f.split('_')
            name = n[1]+'_'+n[2]+'_'+n[3]+'_'+n[4]
            num = n[5]
            pass
        else: continue
        input_file = SPLITLOC + f
        if num == '00':
            DFList = t2n.createDF(input_file,old=old)
            header_file = input_file
        else:
            header_file = SPLITLOC+'header_%s.txt'%name
            DFList = t2n.createDF(input_file,header_file=header_file,old=old)
        t2n.process(SPLITLOC,input_file,DFList,f,header_file=header_file,old=old)
            
def TS_split_main(TSLOC):
    #split_big_files(TSLOC) ###DO NOT RUN THIS UNLESS YOU INTEND TO CLEAN UP
    header_file = TSLOC+'header.txt'
    DFL = []
    files = os.listdir(TSLOC+'split/')
    files.sort()
    for f in files:
        print('trying to run',f)
        input_file = TSLOC+'split/'+f
        DFL.append(t2n.ts_run(TSLOC,input_file,header_file,old=True))
        if len(DFL)>1:
            DFList,DFL = group_by_day(DFL)
            t2n.process(TSLOC,input_file,DFList,f,header_file=header_file,old=True)
            continue
    return DFL
    
def main():
    TS_split_main(TSLOC)
     
if __name__ =='__main__': 
    main()

