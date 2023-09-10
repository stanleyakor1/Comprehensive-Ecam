import sys
import os
import datetime
import logging
import pandas as pd
import multiprocessing as mp
import sys

import urllib.request
import tarfile
import gzip
import os
import subprocess

# Configure the logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def download_data(args):
    
    try:
        url, filename = args
        logger.info(f"Downloading {filename}...")
        urllib.request.urlretrieve(url, filename)
        logger.info(f"Downloaded {filename}")
        return True

    except Exception as e:
        logger.error(f"Error downloading {filename}: {str(e)}")
        return str(e)

def ERA5(start_date,end_date,\
           data_dl_dirc='./',
           logger=False):

    if type(start_date) == str:
        start_date = pd.to_datetime(start_date)
    if type(end_date) == str:
        end_date = pd.to_datetime(end_date)
                  
    date_range = pd.date_range(start_date, end_date, freq='1D')

    Filenames = []
    url = []
    base = "https://data.rda.ucar.edu/ds633.0/"

    files = [
    "e5.oper.an.pl/{}/e5.oper.an.pl.128_060_pv.ll025sc.{}00_{}23.grb",
    "e5.oper.an.pl/{}/e5.oper.an.pl.128_075_crwc.ll025sc.{}00_{}23.grb",
    "e5.oper.an.pl/{}/e5.oper.an.pl.128_076_cswc.ll025sc.{}00_{}23.grb",
    "e5.oper.an.pl/{}/e5.oper.an.pl.128_129_z.ll025sc.{}00_{}23.grb",
    "e5.oper.an.pl/{}/e5.oper.an.pl.128_130_t.ll025sc.{}00_{}23.grb",
    "e5.oper.an.pl/{}/e5.oper.an.pl.128_131_u.ll025uv.{}00_{}23.grb",
    "e5.oper.an.pl/{}/e5.oper.an.pl.128_132_v.ll025uv.{}00_{}23.grb",
    "e5.oper.an.pl/{}/e5.oper.an.pl.128_133_q.ll025sc.{}00_{}23.grb",
    "e5.oper.an.pl/{}/e5.oper.an.pl.128_135_w.ll025sc.{}00_{}23.grb",
    "e5.oper.an.pl/{}/e5.oper.an.pl.128_138_vo.ll025sc.{}00_{}23.grb",
    "e5.oper.an.pl/{}/e5.oper.an.pl.128_155_d.ll025sc.{}00_{}23.grb",
    "e5.oper.an.pl/{}/e5.oper.an.pl.128_157_r.ll025sc.{}00_{}23.grb",
    "e5.oper.an.pl/{}/e5.oper.an.pl.128_203_o3.ll025sc.{}00_{}23.grb",
    "e5.oper.an.pl/{}/e5.oper.an.pl.128_246_clwc.ll025sc.{}00_{}23.grb",
    "e5.oper.an.pl/{}/e5.oper.an.pl.128_247_ciwc.ll025sc.{}00_{}23.grb",
    "e5.oper.an.pl/{}/e5.oper.an.pl.128_248_cc.ll025sc.{}00_{}23.grb",
    ]
    for date in date_range:
        year_mon = date.strftime('%Y%m')
        year_mon_day = date.strftime('%Y%m%d')

        for file in files:
            inurl  = file.format(year_mon, year_mon_day,year_mon_day)
            destination_name = inurl.split("/")
            Filenames.append(destination_name[-1])
            url.append(base + inurl)

    return Filenames, url

if __name__ == "__main__":
    Filenames, Url= ERA5('2023-01-01','2023-01-02')

    with mp.Pool(processes=int(sys.argv[1])) as pool:
        results_async = pool.map_async(download_data, zip(Url, Filenames))

        pool.close()
        pool.join()

    results = results_async.get()

#     base = "https://data.rda.ucar.edu/ds633.0/"
#     files = [
#     "e5.oper.an.pl/202301/e5.oper.an.pl.128_060_pv.ll025sc.2023010100_2023010123.grb",
#     "e5.oper.an.pl/202301/e5.oper.an.pl.128_075_crwc.ll025sc.2023010100_2023010123.grb",
#     "e5.oper.an.pl/202301/e5.oper.an.pl.128_076_cswc.ll025sc.2023010100_2023010123.grb",
#     "e5.oper.an.pl/202301/e5.oper.an.pl.128_129_z.ll025sc.2023010100_2023010123.grb",
#     "e5.oper.an.pl/202301/e5.oper.an.pl.128_130_t.ll025sc.2023010100_2023010123.grb",
#     "e5.oper.an.pl/202301/e5.oper.an.pl.128_131_u.ll025uv.2023010100_2023010123.grb",
#     "e5.oper.an.pl/202301/e5.oper.an.pl.128_132_v.ll025uv.2023010100_2023010123.grb",
#     "e5.oper.an.pl/202301/e5.oper.an.pl.128_133_q.ll025sc.2023010100_2023010123.grb",
#     "e5.oper.an.pl/202301/e5.oper.an.pl.128_135_w.ll025sc.2023010100_2023010123.grb",
#     "e5.oper.an.pl/202301/e5.oper.an.pl.128_138_vo.ll025sc.2023010100_2023010123.grb",
#     "e5.oper.an.pl/202301/e5.oper.an.pl.128_155_d.ll025sc.2023010100_2023010123.grb",
#     "e5.oper.an.pl/202301/e5.oper.an.pl.128_157_r.ll025sc.2023010100_2023010123.grb",
#     "e5.oper.an.pl/202301/e5.oper.an.pl.128_203_o3.ll025sc.2023010100_2023010123.grb",
#     "e5.oper.an.pl/202301/e5.oper.an.pl.128_246_clwc.ll025sc.2023010100_2023010123.grb",
#     "e5.oper.an.pl/202301/e5.oper.an.pl.128_247_ciwc.ll025sc.2023010100_2023010123.grb",
#     "e5.oper.an.pl/202301/e5.oper.an.pl.128_248_cc.ll025sc.2023010100_2023010123.grb",
#     "e5.oper.an.pl/202301/e5.oper.an.pl.128_060_pv.ll025sc.2023010200_2023010223.grb",
#     "e5.oper.an.pl/202301/e5.oper.an.pl.128_075_crwc.ll025sc.2023010200_2023010223.grb",
#     "e5.oper.an.pl/202301/e5.oper.an.pl.128_076_cswc.ll025sc.2023010200_2023010223.grb",
#     "e5.oper.an.pl/202301/e5.oper.an.pl.128_129_z.ll025sc.2023010200_2023010223.grb",
#     "e5.oper.an.pl/202301/e5.oper.an.pl.128_130_t.ll025sc.2023010200_2023010223.grb",
#     "e5.oper.an.pl/202301/e5.oper.an.pl.128_131_u.ll025uv.2023010200_2023010223.grb",
#     "e5.oper.an.pl/202301/e5.oper.an.pl.128_132_v.ll025uv.2023010200_2023010223.grb",
#     "e5.oper.an.pl/202301/e5.oper.an.pl.128_133_q.ll025sc.2023010200_2023010223.grb",
#     "e5.oper.an.pl/202301/e5.oper.an.pl.128_135_w.ll025sc.2023010200_2023010223.grb",
#     "e5.oper.an.pl/202301/e5.oper.an.pl.128_138_vo.ll025sc.2023010200_2023010223.grb",
#     "e5.oper.an.pl/202301/e5.oper.an.pl.128_155_d.ll025sc.2023010200_2023010223.grb",
#     "e5.oper.an.pl/202301/e5.oper.an.pl.128_157_r.ll025sc.2023010200_2023010223.grb",
#     "e5.oper.an.pl/202301/e5.oper.an.pl.128_203_o3.ll025sc.2023010200_2023010223.grb",
#     "e5.oper.an.pl/202301/e5.oper.an.pl.128_246_clwc.ll025sc.2023010200_2023010223.grb",
#     "e5.oper.an.pl/202301/e5.oper.an.pl.128_247_ciwc.ll025sc.2023010200_2023010223.grb",
#     "e5.oper.an.pl/202301/e5.oper.an.pl.128_248_cc.ll025sc.2023010200_2023010223.grb",
# ]

# ALL = []
# for file in files:
#     cfile = base + file
#     ALL.append(cfile)

# for x,y in zip(Url, ALL):
#     if x == y:
#         print(True)
#     else:
#         print(False)
#         print(x)
#         print('\n')
#         print(y)