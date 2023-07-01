import sys
import os
import datetime
import logging
import pandas as pd
import multiprocessing as mp
from download import *


def CFSRV2(start_date,start_hour,
           end_date,end_hour,
           data_dl_dirc='./',
           logger=False):
    
    # Author: William Rudisil
    # Modified: 30 June 2023 by Stanley Akor
    
    # Notes:
    # CFSRV2 is available from 2011-- onwards. The filenames that the ftp server spits out
    # are not unique, meaning that they must be assigned the correct name convention after
    # downloading (unlike CFSR, where the name is unique and correct). See the example URL

    # Example URL
    # https://nomads.ncdc.noaa.gov/modeldata/cfsv2_analysis_flxf/2011/201104/20110401/cdas1.t18z.sfluxgrbf06.grib2
    if type(start_date) == str:
        start_date = pd.to_datetime(start_date)
    if type(end_date) == str:
        end_date = pd.to_datetime(end_date)
                  
    #nomads_url = "https://www.ncei.noaa.gov/thredds/fileServer/model-cfs_v2_anl_6h_pgb/2020/202004/20200429/"
    nomads_url = "https://www.ncei.noaa.gov/thredds/fileServer/model-cfs_v2_anl_6h_{}/{}/{}/{}/"
    # OLD nomads_url = "https://nomads.ncdc.noaa.gov/modeldata/cfsv2_analysis_{}/{}/{}{}/{}{}{}/"
    filename = "cdas1.t{}z.{}.grib2"
    st = datetime.timedelta(hours=start_hour)
    end = datetime.timedelta(hours=end_hour)
    date_range = pd.date_range(start_date + st, end_date + end, freq='6H')

    # Assert extension == 'pgbh' or extension == 'flxf', 'bad argument'
    dlist = []
    filelist = []
    renamelist = []
    for date in date_range:
        for extension in ['pgb']: #'flxf'
            if extension == 'pgb':
                fname_extension = 'pgrbh06'
                rename_extension = 'pgbh06'  # this is to be consisten w/ cfsr .... confusing i know. 
                
            #elif extension == 'flxf':
            #    fname_extension = 'sfluxgrbf06'
            #    rename_extension = 'flxf06'  # this is to be consisten w/ cfsr .... confusing i know. 
            # get date info duh 
            year = date.strftime('%Y')
            month = date.strftime('%m')
            day = date.strftime('%d')
            hour = date.strftime('%H')
            # get the pgbh files
            base = nomads_url.format(extension, year, year+ month, year+month+day)
            filename_complete = filename.format(hour, fname_extension)
            filepath = base + filename_complete
            rename = "{}_{}{}{}{}_{}".format(rename_extension, year, month, day, hour, filename_complete)

            # create lists of each
            dlist.append(filepath)
            filelist.append(filename_complete)
            renamelist.append(rename)
            # append rename to list 
   # print(dlist)
    return dlist, filelist, renamelist



if __name__ == "__main__":
    dlist, filelist, renamelist = CFSRV2("2023-01-11", 12, "2023-01-12", 0, data_dl_dirc='./', logger=False)

    with mp.Pool(processes=8) as pool:
        results_async = pool.map_async(download_data, zip(dlist, renamelist))

        pool.close()
        pool.join()

    results = results_async.get()
