import xarray as xr
import xesmf as xe
import math
import numpy as np
from scipy.stats import spearmanr
import pathlib as pl
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.colors as colors
import matplotlib.patches as patches
import glob
import subprocess
from netCDF4 import Dataset
import os
import multiprocessing as mp
import sys
import shutil
import warnings
import matplotlib.dates as mdates
from mpl_toolkits.axes_grid1 import ImageGrid
import warnings
from mpl_toolkits.axes_grid1 import make_axes_locatable
import torch
from scipy.stats import pearsonr

from wrf import (getvar, to_np, get_cartopy, latlon_coords, vertcross, ll_to_xy,
                 cartopy_xlim, cartopy_ylim, interpline, CoordPair, destagger, 
                 interplevel)
from matplotlib.colors import LogNorm
# Block all future warnings
warnings.filterwarnings("ignore")



'''
Convert station lat/lon coordinate to wrf x/y point 

'''
def get_wrf_xy(geog,lat, lon):
    #lat = 38.89
    #lon = -106.95
    xlat = geog.XLAT.values[0,:,:]
    xlon = geog.XLONG.values[0,:,:]
    dist = np.sqrt((xlat - lat)**2 + (xlon - lon)**2)
    mindist = dist.min()
    ixlat = np.argwhere(dist == mindist)[0][0]
    ixlon = np.argwhere(dist == mindist)[0][1]
    return ixlat, ixlon

'''
Regrid snodas to wrf resolution
'''
def regrid_snodas(wrf_file, snodas_file, var, multiple_time = True):
    ds_snodas = xr.open_dataset(snodas_file)
    
    if multiple_time:
        ds_snodas= ds_snodas.swap_dims({'time': 'XTIME'})
    else:   
        ds_snodas = ds_snodas.sel(time = '2022-04-01T06:00:00')
        
    ds_snodas = ds_snodas[var]

    grid_template = xr.open_dataset(wrf_file)
    LAT = grid_template.variables['XLAT'][0].copy()
    LON =grid_template.variables['XLONG'][0].copy()

    #create target grid
    target_grid = xr.Dataset({'lat': LAT,'lon': LON})

    # Create xESMF regridder using the target grid
    regridder = xe.Regridder(ds_snodas, target_grid, 'bilinear')
    return regridder(ds_snodas)



#file = precip_accumulate(path,'WSM6_22_daily.nc',['TMN','I_RAINNC','SNOWH','RAINNC','T2','SNOW','U10','V10'])
#file.main()
'''

Accumulate precipitation from raw wrf output file.

'''
class precip_accumulate():
    def __init__(self,path, save_name, var_list):
        self.save_name = save_name
        self.var_list = var_list
        self.wrf = xr.open_dataset(path)
        self.wrf = self.wrf.swap_dims({'Time':'XTIME'})
        self.new_array = self.wrf[self.var_list].resample(XTIME = '24H').mean(dim = 'XTIME') # create daily means of few variables
        
    def calc_precip(self):
        return  self.wrf['RAINNC']+self.wrf['I_RAINNC']* 100.0

    # # Daily Aggregations (hourly to daily)
    def main(self):
        self.wrf['PRCP'] = self.wrf['RAINNC']
        self.wrf['PRCP'].values = self.calc_precip()
        
        #Compute accumulative precip for each day
        self.new_array['PRCP'] = self.wrf['PRCP'].resample(XTIME = '24H').max(dim = 'XTIME')
        
        self.new_array.to_netcdf('/bsuhome/stanleyakor/wateryear_2022/'+self.save_name)
        
        
def prep_taylor(th,mor,wsm6,wdm6, sn_regrid_sh,type):
    
    if type == 'SNOWH':
        th_spatial_mean = th['SNOWH'].mean(dim = ('south_north', 'west_east'))[2:-1]*1e3
        mor_spatial_mean = mor['SNOWH'].mean(dim = ('south_north', 'west_east'))[2:-1]*1e3
        wsm6_spatial_mean = wsm6['SNOWH'].mean(dim = ('south_north', 'west_east'))[2:-1]*1e3
        wdm6_spatial_mean = wdm6['SNOWH'].mean(dim = ('south_north', 'west_east'))[2:-1]*1e3
        snodas_sp_mean = sn_regrid_sh.mean(dim = ('south_north', 'west_east'))
        
    else:
        th_spatial_mean = th['SNOW'].mean(dim = ('south_north', 'west_east'))[2:-1]
        snodas_sp_mean = sn_regrid_sh.mean(dim = ('south_north', 'west_east'))
        mor_spatial_mean = mor['SNOW'].mean(dim = ('south_north', 'west_east'))[2:-1]
        wsm6_spatial_mean = wsm6['SNOW'].mean(dim = ('south_north', 'west_east'))[2:-1]
        wdm6_spatial_mean = wdm6['SNOW'].mean(dim = ('south_north', 'west_east'))[2:-1]


    samples = [[float(np.std(th_spatial_mean)),np.corrcoef(snodas_sp_mean,th_spatial_mean)[0,1], "Thom"],  
            [float(np.std(mor_spatial_mean)),np.corrcoef(snodas_sp_mean,mor_spatial_mean)[0,1], "Mor"],  
            [float(np.std(wsm6_spatial_mean)),np.corrcoef(snodas_sp_mean,wsm6_spatial_mean)[0,1], "WSM6"], 
            [float(np.std(wdm6_spatial_mean)),np.corrcoef(snodas_sp_mean,wdm6_spatial_mean)[0,1], "WDM6"]]


    return float(np.std(snodas_sp_mean)), samples

'''
Compute mean absolute error
'''
def calculate_mae(observed, predicted):
    ae = np.abs(observed - predicted)
    return   ae.mean(dim=('south_north', 'west_east'))

'''
Compute R2 error

'''
def calculate_r_squared(observed, predicted):
    mean_observed = observed.mean(dim = ('south_north', 'west_east'))
    ssr = (observed - predicted) ** 2
    ssr = ssr.sum(dim =  ('south_north', 'west_east'))
    sst = (observed - mean_observed) ** 2
    sst = sst.sum(dim =  ('south_north', 'west_east'))
    r_squared = 1 - (ssr / sst)
    return r_squared
