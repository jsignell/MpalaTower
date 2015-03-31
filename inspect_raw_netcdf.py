# -*- coding: utf-8 -*-
"""
Created on Tue Mar 31 14:51:12 2015

@author: Julia
"""
from __future__ import print_function
import pandas as pd
import numpy as np
import datetime as dt
import matplotlib.pyplot as plt
import os
import xray

usr = 'Julia'
NETCDFLOC = 'C:/Users/%s/Dropbox (PE)/erddap/data/'%usr

# Enter start and end dates as ISO strings or leave blank to concatenate 
# across all available time. data refers to the datafile name, choose from:
# ['lws','licor','WVIA','ts_data','flux','upper','Manifold','Table1','Table1_rain']
def grabDateRange(input_dir,data,start='2010-01-01',end=dt.datetime.now()):
    rng = pd.date_range(start,end,freq='D')
    filerng = ['raw_MpalaTower_%04d_%03d.nc'%
               (date.year,date.dayofyear) for date in rng]
    ds_list = []
    fileNames = []
    FILEDIR = input_dir+data+'/'
    for fileName in set(filerng) & set(os.listdir(FILEDIR)):
        fileNames.append(fileName)
    fileNames.sort()
    for fileName in fileNames:
        ds_list.append(xray.open_dataset(FILEDIR+fileName,decode_times=True)) 
    ds = xray.Dataset()
    ds = xray.concat((ds_list[0:]),dim='time')
    return ds
    
def process(): 
    print('Which datafile do you want to look at (choose from list and use exact spelling)? ')
    data = str(raw_input('lws, licor, WVIA, ts_data, flux, upper, Manifold, Table1, Table1_rain\n'))
    input_dir = NETCDFLOC
    print('If you don\'t want the full dataset, enter date range in format YYYY-MM-DD')
    start = str(raw_input('start: '))
    end = str(raw_input('end: '))
    try: ds = grabDateRange(input_dir,data,start=start,end=end)
    except: ds = grabDateRange(input_dir,data)
    return ds,start,end
    
def clean_Table1(ds):
    L = list(ds.vars)
    bad = ['broken','bad','moved']
    L = [s for s in L if 'Avg' in s]
    for b in bad:
        L = [s for s in L if b not in s]
    #L = [s.replace('Gass','Grass') for s in L]
    #L = [s.replace('open','Open') for s in L]
    places = ['Tree','Grass','Riparian','Open']
    ps = [(0,0),(0,1),(1,0),(1,1)]
    depths = ['05cm','10cm','20cm','30cm','100cm']
    colors = ['r','b','g','c','purple']
    data_options = ['VW','PA','Tsoil','shf']
    return L,places,ps,depths,colors,data_options
    
def pick_type(L,data_options):
    print('Which of the following types of data would you like to explore?: ')
    data = str(raw_input(data_options))
    data_list = [k for k in L if data in k]
    bad_list = ['Mes', 'mV', 'del', 'cal']
    for bad in bad_list:
        data_list = [k for k in data_list if bad not in k]
    return data,data_list
    
def make_plots(input_dir,ds,start,end,places,ps,depths,colors,data,data_list):
    fig, axes = plt.subplots(nrows=2,ncols=2,sharex=True,sharey=True,figsize=(14,8))
    data_list.sort()
    for i in data_list:
        label = 'no depth'
        color = 'k'
        place = ''
        for place in places:
            if place in i or place.lower() in i: 
                p = ps[places.index(place)]
                axes[p].set_title(place)
        plt.sca(axes[p])
        for depth in depths:
            if depth in i:
                color = colors[depths.index(depth)]
                label = depth
        plt.plot(ds['time'],ds[i],color,label=label)
        if p == (0,1): 
            plt.legend(bbox_to_anchor=(-.45, 1.2, 1., 0.07), loc=1, ncol=5, borderaxespad=0.)
    title = '%s from %s to %s'%(data,start,end)
    fig = fig.suptitle('\n'+title,fontsize=16,y=1.1)
    plt.savefig(input_dir+'plots/'+title+'.jpg', bbox_inches='tight')
    return fig