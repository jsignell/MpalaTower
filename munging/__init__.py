'''

Julia Signell
2015-05-07
'''


import os
import shutil
import sys
import datetime as dt
import posixpath
import pandas as pd
import numpy as np
import xray
import zipfile
import matplotlib.pyplot as plt


datafiles = ['Table1', 'flux', 'Manifold', 'WVIA',
             'lws', 'upper', 'ts_data', 'soil']

coords = dict(
    station_name='MPALA Tower',
    site=None,
    lon=36.8701,      # degrees east
    lat=0.4856,       # degrees north
    elevation=1610    # m above sea level
    )

attrs = dict(
    title = 'Flux Tower Data from MPALA',
    summary = ('This raw data comes from the MPALA Flux Tower, which is ' +
               'maintained by the Ecohydrology Lab at Mpala Research Centre ' +
               'in Laikipia, Kenya. It is part of a long-term monitoring ' +
               'project that was originally funded by NSF and now runs with ' +
               'support from Princeton. Its focus is on using stable ' +
               'isotopes to better understand water balance in drylands, ' +
               'particularly transpiration and evaporation fluxes.'),
    license = 'MIT License',
    institution = 'Princeton University',
    acknowledgement = 'Funded by NSF and Princeton University',
    naming_authority = 'caylor.princeton.edu',
    id = 'MPALA Tower',
    creator_name = 'Kelly Caylor',
    creator_email = 'kcaylor@princeton.edu',
    keywords = 'eddy covariance, isotope hydrology, land surface flux',
    Conventions = 'CF-1.6',
    featureType = 'timeSeries',
    local_timezone = 'Africa/Nairobi'
    )
