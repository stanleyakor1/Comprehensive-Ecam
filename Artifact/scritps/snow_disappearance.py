from packages import *
from domain import *
import logging

# Configure the logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
console_handler = logging.StreamHandler()
console_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler.setFormatter(console_formatter)
logger.addHandler(console_handler)

def calculate_sdd(da, var):
    da = da[var]
    empty_data_array = np.zeros((da.shape[1], da.shape[2]))
    
    # Get time coordinate values from the input file
    time_values = np.arange(0, da.shape[0])

    # Loop through latitude and longitude dimensions
    for i in range(da.shape[1]):
        for j in range(da.shape[2]):
            snow_disappearance_date = None
            consecutive_snow_days = 0
            consecutive_nonsnow_days = 0

            # Loop backward through time
            for t in range(da.shape[0] - 1, -1, -1):
                current_snow_height = da.isel(XTIME=t, south_north=i, west_east=j).values

                # Add log statements for troubleshooting
                logger.info(f"Current snow height at ({i}, {j}, {t}): {current_snow_height}")

                if current_snow_height > 0:
                    consecutive_snow_days += 1
                    consecutive_nonsnow_days = 0
                else:
                    consecutive_nonsnow_days += 1
                    consecutive_snow_days = 0

                if consecutive_snow_days >= 5:
                    # Loop forward to find the consecutive dry days
                    consecutive_nonsnow_days2 = 0
                    for ts in range(t, da.shape[0]):
                        current_snow_height2 = da.isel(XTIME=ts, south_north=i, west_east=j).values

                        if current_snow_height2 == 0:
                            consecutive_nonsnow_days2 += 1
                        else:
                            consecutive_nonsnow_days2 = 0

                        if consecutive_nonsnow_days2 >= 5:
                            snow_disappearance_date = time_values[ts]  # Assuming time_values is a pandas datetime object
                            break

                    if snow_disappearance_date is not None:
                        logger.info(f"Snow disappearance date found at ({i}, {j}): {snow_disappearance_date}")
                    else:
                        logger.info(f"No snow disappearance date found at ({i}, {j})")
                
                if snow_disappearance_date is not None:
                    break  # Exit the loop once snow disappearance date is found

            empty_data_array[i, j] = snow_disappearance_date

    return empty_data_array

def compute(path, save_name):
    data = xr.open_dataset(path)
    data1 = data.isel(XTIME=0)
    
    data1['SDD'] = data1['SNOWH']
    data1['SDD'].values = calculate_sdd(data, 'SNOWH')
    new_array = data1['SDD']
    
    new_array.to_netcdf('/bsuhome/stanleyakor/wateryear_2022/' + save_name)

path = '/bsuhome/stanleyakor/wateryear_2022/WSM6_CFS_23_daily.nc'

compute(path,'SDD_WSM6_CFS.nc')
