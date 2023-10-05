import xarray as xr
import numpy as np
import time
import logging

# Configure the logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)

# Define a function to replace NaN with the average of neighbors
def replace_nan_with_avg(da):
    mask = np.isnan(da)
    for t in range(da.shape[0]):  # Loop through time dimension
        time1 = f'Looping through time = {t}, {da.shape[0] - t} time steps left'
        logger.info(time1)
        for i in range(da.shape[1]):  # Loop through latitude dimension
            for j in range(da.shape[2]):  # Loop through longitude dimension
                if mask[t, i, j]:
                    neighbors = []
                    for dt in [-1, 0, 1]:
                        for di in [-1, 0, 1]:
                            for dj in [-1, 0, 1]:
                                nt, ni, nj = t + dt, i + di, j + dj
                                if (
                                    0 <= nt < da.shape[0]
                                    and 0 <= ni < da.shape[1]
                                    and 0 <= nj < da.shape[2]
                                    and not np.isnan(da[nt, ni, nj])
                                ):
                                    neighbors.append(da[nt, ni, nj])
                    if neighbors:
                        da[t, i, j] = np.mean(neighbors)

start = time.time()
# Load the NetCDF file
nc_file_path = '/bsuhome/stanleyakor/wateryear_2022/SNOWH_snodas_wateryear_2022_d02.nc'
ds = xr.open_dataset(nc_file_path)

# Call the function
replace_nan_with_avg(ds['SNWZ'])

# Write new data to file.
output_nc_file_path = '/bsuhome/stanleyakor/Python-tutorial/wrf-python/SNWZ_nonan_snodas_WY_22.nc'
ds.to_netcdf(output_nc_file_path)
end = time.time()

logger.info(f"It took {end - start} seconds to complete this task!")
