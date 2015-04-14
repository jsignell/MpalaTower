#Script:split_big_files.py
#       Splits big data files from mpala tower so that the script can run with
#       out crashing the computer. Also is 
#Modified by: Julia Signell
#Date created: 2015-04-09
#Date modified: 2015-04-13

import os
import pandas as pd
import tower_to_netcdf as t2n
from posixpath import join
import shutil

usr = 'Julia'
ROOTDIR = 'C:/Users/%s/Dropbox (PE)/KenyaLab/Data/Tower/'%usr
DATALOC = ROOTDIR + 'TowerData/'
TSLOC = DATALOC + 'CR3000_SN4709_ts_data/'
NETCDFLOC = DATALOC+'raw_netCDF_output/'

# To run the program foo that takes an option and two arguments
# Equivalent to "foo -d bar baz" directly in the shell
#call(['split', '-d', 'bar', 'baz'])

            #n = f.split('_')
            #split_file = 'split_'+n[2]+'_'+n[3]+'_'+n[4]+'_'+n[5]

def split_big_files(input_dir,datas,header_file=None):
    #BEWARE THAT THERE ARE BACKUP FILES WITH THE FIRST 3000 LINES BELONGING AT
    #THE END. YOU NEED TO CORRECT FOR THIS USING CYGWIN COMMANDS
    k=0
    little_files = []
    if not os.path.exists(join(input_dir,'split')):
        os.mkdir(join(input_dir,'split'))
    data = None
    for f in os.listdir(input_dir):
        conditions = ('.zip' not in f, 'ts_data' not in f, f.startswith('.'))
        if conditions == (True,True,False): pass
        else: continue        
        #    header_file = join(TSLOC, 'header.txt')
        if header_file == None:
            header_file = join(input_dir, f)
        if any(data in f for data in datas):
            data = [data for data in datas if data in f][0]
        else:
            print('%s has an unknown datatype'%f)
            continue
        print(f)
        statinfo = os.stat(join(input_dir,f))
        file_size = statinfo.st_size 
        if file_size >= 200000000:
            print(file_size)
            in_out_f = '%s_%02d'%(join(input_dir,f)+' '+join(input_dir,'split',data),k)
            os.system(r'C:\cygwin\bin\bash.exe --login -c "split -C 180M -d %s"'%
                      in_out_f) 
            print('processed %s'%in_out_f)
            k+=1
        else:
            little_files.append(f)
    print(little_files)
    return little_files

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
    
def flux_split_main(input_dir,output_dir,datas,old=True):
    #this is for processing files that are larger than 400MB. First split them
    #using the cygwin commands described and then run this program.
    print 'running splitmain with input_dir = %s'%input_dir
    little_files = split_big_files(input_dir,datas)
    try: 
        processed = open(join(input_dir,'processed2netCDF.txt'),'r').read() 
    except: processed = []
    for f in [f for f in little_files if f not in processed]:
        input_file = join(input_dir,f)
        DFList = t2n.createDF(input_file,old=old)
        t2n.process(input_dir,input_file,DFList,f,old=old)
    DFL = []
    files = os.listdir(join(input_dir,'split'))
    files.sort()
    for f in [f for f in files if f not in processed]:
        print('trying to run',f)
        input_file = join(input_dir,'split',f)
        if f.endswith('00'):
            DFList = t2n.createDF(input_file,old=old)
            header_file = input_file
            DFL.append(DFList)
        else:
            DFList = t2n.createDF(input_file,header_file=header_file,old=old)
        DFL.append(DFList)
        if len(DFL)>1:
            DFList,DFL = group_by_day(DFL)
            t2n.process(input_dir,input_file,DFList,f,header_file=header_file,old=old)
        t2n.process(input_dir,input_file,DFList,f,header_file=header_file,old=old)
    try: shutil.rmtree(join(input_dir,'split/'))
    except: pass
            
def TS_split_main(input_dir,output_dir,old=True):
    #split_big_files(input_dir) ###DO NOT RUN THIS UNLESS YOU INTEND TO CLEAN UP
    header_file = join(input_dir,'header.txt')
    DFL = []
    files = os.listdir(join(input_dir,'split'))
    files.sort()
    processed = open(join(input_dir,'processed2netCDF.txt'),'r').read() 
    for f in [f for f in files if f not in processed]:
        print('trying to run',f)
        input_file = join(input_dir,'split',f)
        DFL.append(t2n.ts_run(input_dir,input_file,header_file,old=old))
        if len(DFL)>1:
            DFList,DFL = group_by_day(DFL)
            t2n.process(input_dir,input_file,DFList,f,header_file=header_file,old=old)

def flux_archive_search(input_dir,output_dir,old=True):
    #steps down through messy folders and finds interesting files
    datas = ['lws', 'licor', 'WVIA', 'flux', 
             'upper', 'Manifold', 'Table1', 'Table1Rain']
    for path,dir,files in os.walk(input_dir):
        if any(data in file for file in files for data in datas): 
        # checks if any data files are in a dir
            if 'split' not in path:
                path = path.replace('\\','/')
                flux_split_main(path,output_dir,datas,old=True)

def main():
    #TS_split_main(TSLOC,NETCDFLOC,old=True)
    dirs = [dir for dir in os.listdir('F:loggernet/')]
    for input_dir in dirs:
        try: flux_archive_search('F:/loggernet/'+input_dir,NETCDFLOC,old=True)
        except: print('can\'t handle %s'%input_dir)
        processed = open(join('F:/loggernet','processed2netCDF.txt'),'a')
        processed.write(input_dir+'\n')
        processed.close
    
    t2n.createSummaryTable(NETCDFLOC)
if __name__ =='__main__': 
    main()

