import sys
import os
from datetime import datetime, timedelta
import logging
import pandas as pd
import multiprocessing as mp
import sys

import urllib.request
import tarfile
import gzip
import os
import subprocess

#----------------------------------------------------------------------
# For certificate download issues!                                      

import ssl  # Import the ssl module

# Disable SSL certificate verification
ssl._create_default_https_context = ssl._create_unverified_context

#----------------------------------------------------------------------


# Configure the logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

import calendar

 # Determine the number of days in the month
def last_day_of_month(year, month):
   
    _, last_day = calendar.monthrange(year, month)
    return last_day


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

def ERA5_surface(start_date,end_date,\
           data_dl_dirc='./',
           logger=False):
    
    if type(start_date) == str:
        start_date = pd.to_datetime(start_date)
    if type(end_date) == str:
        end_date = pd.to_datetime(end_date)
                  
    date_range = pd.date_range(start_date, end_date, freq='1M')
    
    Filenames = []
    url = []
    base = "https://data.rda.ucar.edu/ds633.0/"


    files = [
        "e5.oper.an.sfc/{}/e5.oper.an.sfc.128_015_aluvp.ll025sc.{}0100_{}23.grb",
        "e5.oper.an.sfc/{}/e5.oper.an.sfc.128_016_aluvd.ll025sc.{}0100_{}23.grb",
        "e5.oper.an.sfc/{}/e5.oper.an.sfc.128_017_alnip.ll025sc.{}0100_{}23.grb",
        "e5.oper.an.sfc/{}/e5.oper.an.sfc.128_018_alnid.ll025sc.{}0100_{}23.grb",
        "e5.oper.an.sfc/{}/e5.oper.an.sfc.128_031_ci.ll025sc.{}0100_{}23.grb",
        "e5.oper.an.sfc/{}/e5.oper.an.sfc.128_032_asn.ll025sc.{}0100_{}23.grb",
        "e5.oper.an.sfc/{}/e5.oper.an.sfc.128_033_rsn.ll025sc.{}0100_{}23.grb",
        "e5.oper.an.sfc/{}/e5.oper.an.sfc.128_034_sstk.ll025sc.{}0100_{}23.grb",
        "e5.oper.an.sfc/{}/e5.oper.an.sfc.128_035_istl1.ll025sc.{}0100_{}23.grb",
        "e5.oper.an.sfc/{}/e5.oper.an.sfc.128_036_istl2.ll025sc.{}0100_{}23.grb",
        "e5.oper.an.sfc/{}/e5.oper.an.sfc.128_037_istl3.ll025sc.{}0100_{}23.grb",
        "e5.oper.an.sfc/{}/e5.oper.an.sfc.128_038_istl4.ll025sc.{}0100_{}23.grb",
        "e5.oper.an.sfc/{}/e5.oper.an.sfc.128_039_swvl1.ll025sc.{}0100_{}23.grb",
        "e5.oper.an.sfc/{}/e5.oper.an.sfc.128_040_swvl2.ll025sc.{}0100_{}23.grb",
        "e5.oper.an.sfc/{}/e5.oper.an.sfc.128_041_swvl3.ll025sc.{}0100_{}23.grb",
        "e5.oper.an.sfc/{}/e5.oper.an.sfc.128_042_swvl4.ll025sc.{}0100_{}23.grb",
        "e5.oper.an.sfc/{}/e5.oper.an.sfc.128_059_cape.ll025sc.{}0100_{}23.grb",
        "e5.oper.an.sfc/{}/e5.oper.an.sfc.128_066_lailv.ll025sc.{}0100_{}23.grb",
        "e5.oper.an.sfc/{}/e5.oper.an.sfc.128_067_laihv.ll025sc.{}0100_{}23.grb",
        "e5.oper.an.sfc/{}/e5.oper.an.sfc.128_078_tclw.ll025sc.{}0100_{}23.grb",
        "e5.oper.an.sfc/{}/e5.oper.an.sfc.128_079_tciw.ll025sc.{}0100_{}23.grb",
        "e5.oper.an.sfc/{}/e5.oper.an.sfc.128_134_sp.ll025sc.{}0100_{}23.grb",
        "e5.oper.an.sfc/{}/e5.oper.an.sfc.128_136_tcw.ll025sc.{}0100_{}23.grb",
        "e5.oper.an.sfc/{}/e5.oper.an.sfc.128_137_tcwv.ll025sc.{}0100_{}23.grb",
        "e5.oper.an.sfc/{}/e5.oper.an.sfc.128_139_stl1.ll025sc.{}0100_{}23.grb",
        "e5.oper.an.sfc/{}/e5.oper.an.sfc.128_141_sd.ll025sc.{}0100_{}23.grb",
        "e5.oper.an.sfc/{}/e5.oper.an.sfc.128_148_chnk.ll025sc.{}0100_{}23.grb",
        "e5.oper.an.sfc/{}/e5.oper.an.sfc.128_151_msl.ll025sc.{}0100_{}23.grb",
        "e5.oper.an.sfc/{}/e5.oper.an.sfc.128_159_blh.ll025sc.{}0100_{}23.grb",
        "e5.oper.an.sfc/{}/e5.oper.an.sfc.128_164_tcc.ll025sc.{}0100_{}23.grb",
        "e5.oper.an.sfc/{}/e5.oper.an.sfc.128_165_10u.ll025sc.{}0100_{}23.grb",
        "e5.oper.an.sfc/{}/e5.oper.an.sfc.128_166_10v.ll025sc.{}0100_{}23.grb",
        "e5.oper.an.sfc/{}/e5.oper.an.sfc.128_167_2t.ll025sc.{}0100_{}23.grb",
        "e5.oper.an.sfc/{}/e5.oper.an.sfc.128_168_2d.ll025sc.{}0100_{}23.grb",
        "e5.oper.an.sfc/{}/e5.oper.an.sfc.128_170_stl2.ll025sc.{}0100_{}23.grb",
        "e5.oper.an.sfc/{}/e5.oper.an.sfc.128_183_stl3.ll025sc.{}0100_{}23.grb",
        "e5.oper.an.sfc/{}/e5.oper.an.sfc.128_186_lcc.ll025sc.{}0100_{}23.grb",
        "e5.oper.an.sfc/{}/e5.oper.an.sfc.128_187_mcc.ll025sc.{}0100_{}23.grb",
        "e5.oper.an.sfc/{}/e5.oper.an.sfc.128_188_hcc.ll025sc.{}0100_{}23.grb",
        "e5.oper.an.sfc/{}/e5.oper.an.sfc.128_198_src.ll025sc.{}0100_{}23.grb",
        "e5.oper.an.sfc/{}/e5.oper.an.sfc.128_206_tco3.ll025sc.{}0100_{}23.grb",
        "e5.oper.an.sfc/{}/e5.oper.an.sfc.128_229_iews.ll025sc.{}0100_{}23.grb",
        "e5.oper.an.sfc/{}/e5.oper.an.sfc.128_230_inss.ll025sc.{}0100_{}23.grb",
        "e5.oper.an.sfc/{}/e5.oper.an.sfc.128_231_ishf.ll025sc.{}0100_{}23.grb",
        "e5.oper.an.sfc/{}/e5.oper.an.sfc.128_232_ie.ll025sc.{}0100_{}23.grb",
        "e5.oper.an.sfc/{}/e5.oper.an.sfc.128_235_skt.ll025sc.{}0100_{}23.grb",
        "e5.oper.an.sfc/{}/e5.oper.an.sfc.128_236_stl4.ll025sc.{}0100_{}23.grb",
        "e5.oper.an.sfc/{}/e5.oper.an.sfc.128_238_tsn.ll025sc.{}0100_{}23.grb",
        "e5.oper.an.sfc/{}/e5.oper.an.sfc.128_243_fal.ll025sc.{}0100_{}23.grb",
        "e5.oper.an.sfc/{}/e5.oper.an.sfc.128_244_fsr.ll025sc.{}0100_{}23.grb",
        "e5.oper.an.sfc/{}/e5.oper.an.sfc.128_245_flsr.ll025sc.{}0100_{}23.grb",
        "e5.oper.an.sfc/{}/e5.oper.an.sfc.228_010_lblt.ll025sc.{}0100_{}23.grb",
        "e5.oper.an.sfc/{}/e5.oper.an.sfc.228_011_ltlt.ll025sc.{}0100_{}23.grb",
        "e5.oper.an.sfc/{}/e5.oper.an.sfc.228_012_lshf.ll025sc.{}0100_{}23.grb",
        "e5.oper.an.sfc/{}/e5.oper.an.sfc.228_013_lict.ll025sc.{}0100_{}23.grb",
        "e5.oper.an.sfc/{}/e5.oper.an.sfc.228_014_licd.ll025sc.{}0100_{}23.grb",
        "e5.oper.an.sfc/{}/e5.oper.an.sfc.228_089_tcrw.ll025sc.{}0100_{}23.grb",
        "e5.oper.an.sfc/{}/e5.oper.an.sfc.228_090_tcsw.ll025sc.{}0100_{}23.grb",
        "e5.oper.an.sfc/{}/e5.oper.an.sfc.228_131_u10n.ll025sc.{}0100_{}23.grb",
        "e5.oper.an.sfc/{}/e5.oper.an.sfc.228_132_v10n.ll025sc.{}0100_{}23.grb",
        "e5.oper.an.sfc/{}/e5.oper.an.sfc.228_246_100u.ll025sc.{}0100_{}23.grb",
        "e5.oper.an.sfc/{}/e5.oper.an.sfc.228_247_100v.ll025sc.{}0100_{}23.grb",
    ]

    
    for mn in date_range:
        month =   mn.strftime('%m')
        year = mn.strftime('%Y')

        last_day = last_day_of_month(int(year), int(month))

        ymd=f'{year}{month}{last_day}'
        ym = f'{year}{month}'
    
        for file in files:
            inurl  = file.format(ym,ym, ymd)
            destination_name = inurl.split("/")
            Filenames.append(destination_name[-1])
            url.append(base + inurl)
            
    return Filenames, url

if __name__ == "__main__":
    Filenames, Url= ERA5_surface('2022-09-29','2023-06-30')

    with mp.Pool(processes=int(sys.argv[1])) as pool:
        results_async = pool.map_async(download_data, zip(Url, Filenames))
        pool.close()
        pool.join()

    results = results_async.get()
