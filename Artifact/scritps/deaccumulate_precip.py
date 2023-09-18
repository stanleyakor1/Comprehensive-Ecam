from packages import *
from domain import *

# Decummulate Precipitation 
def calc_precip(cum_precip, bucket_precip):
    
    total_precip = cum_precip + bucket_precip * 100.0
    PRCP = np.zeros(total_precip.shape)
    
    for i in np.arange(1,PRCP.shape[0]):
        
        PRCP[i,:,:] = total_precip[i,:,:].values - total_precip[i-1,:,:].values
        
    return PRCP




def compute(path,save_name,threshold):
    data = xr.open_dataset(path)
    data = data.swap_dims({'Time':'XTIME'})

    data_irannc = data['I_RAINNC']
    data_rainnc = data['RAINNC']
   
    data['PRCP'] = data['RAINNC']
    data['PRCP'].values = calc_precip(data_rainnc, data_irannc)


    # Does it make sense to use the max instead of the mean?
    new_array =  data['PRCP'].resample(XTIME = '24H').max(dim = 'XTIME')


    # Compute the exceedance count
    exceedance_count = (new_array >= threshold).sum(dim='XTIME')


    # Create a new Dataset to store the results
    result_dataset = xr.Dataset({'exceedance_count': exceedance_count})

    # Create a new NetCDF file for storing the exceedance count data
    result_dataset.to_netcdf('/bsuhome/stanleyakor/wateryear_2022/'+save_name)


path = '/bsuhome/stanleyakor/scratch/2022/'

compute(path+'THOMPSON_wateryear_2022_d02_sliced.nc','Thompson_exceedance',5.0)
compute(path+'WDM6_wateryear_2022_d02_sliced.nc','WDM6_exceedance',5.0)
compute(path+'WSM6_wateryear_2022_d02_sliced.nc','WSM6_exceedance',5.0)
compute(path+'MORRIOSN_wateryear_2022_d02_sliced.nc','Mor_exceedance',5.0)