class getclosest():
    def __init__(self, path_to_header,path_to_csv,path_to_geog,path_to_wrf_file,save_name, save = True):
        self.path_to_header = path_to_header
        self.path_to_csv = path_to_csv
        self.geog = xr.open_dataset(path_to_geog)
        self.dict = {}
        self.wrf = xr.open_dataset(path_to_wrf_file)
        self.wrf_file = self.wrf['PRCP']
        self.start = '2021-10-01'
        self.end = '2022-01-14'
        self.feat = {}
        self.save = save
        self.save_name = save_name
        #self.wrf_path = path_to_wrf_file

    def collect_snodas_info(self):
        df = pd.read_csv(self.path_to_header)
        df = df[(df['Latitude'] >= 42.80) & (df['Longitude'] > -116)]
        filtered_df = df[df['State'] == 'ID']
        sta_names = filtered_df['Station Name'].tolist()
        lat = filtered_df['Latitude'].tolist()
        lon = filtered_df['Longitude'].tolist()
        sta_id = filtered_df['Station ID'].tolist()

        '''
          Seems one of the stations has been 
          decommissioned, so we remove it

        '''
        # element_to_find = 782
        # index = sta_id.index(element_to_find)
        # del lat[index]
        # del lon[index]
        # del sta_names[index]
        # del sta_id[index]
        return lat,lon,sta_names,sta_id

    def get_wrf_xy(self):
        xlat = self.geog.XLAT.values[0,:,:]
        xlon = self.geog.XLONG.values[0,:,:]

        lat, lon, sta_names, sta_id = self.collect_snodas_info()
        
        for x,y,z in zip(lat,lon,sta_id):
            dist = np.sqrt((xlat - x)**2 + (xlon - y)**2)
            mindist = dist.min()
            ixlat = np.argwhere(dist == mindist)[0][0]
            ixlon = np.argwhere(dist == mindist)[0][1]

            self.dict[str(z)] = (ixlat,ixlon)
            
        return self.dict

    def extract_precip(self,ixlat,ixlon):
        return self.wrf_file.isel(south_north = ixlat, west_east = ixlon).values
    
    def read_csv(self):
    
        lat, lon, sta_names, sta_id = self.collect_snodas_info()
        dict = self.get_wrf_xy()
        names = {}
        for id in range(len(sta_id)):
            name = f'df_{sta_id[id]}.csv'
            path = self.path_to_csv+'/'+name
            generic = f'{sta_names[id]} ({sta_id[id]}) Precipitation Accumulation (in) Start of Day Values'
            df = pd.read_csv(path)
            df = df[(df['Date'] >=self.start) & (df['Date'] <= self.end)]
            df_filtered = df[generic].tolist()
            df_filtered = [value * 25.4 for value in df_filtered]
            ixlat,ixlon = dict[str(sta_id[id])]
            wrf_precip = self.extract_precip(ixlat,ixlon)
            wrf_precip = wrf_precip[2:-1]
            bias = (df_filtered - wrf_precip).mean()
            self.feat[sta_id[id]] = bias
            names[sta_id[id]]=sta_names[id]

        # Get the keys with the 5 smallest values
        smallest_keys = sorted(self.feat, key=lambda k: abs(self.feat[k]))[:6]

        # Create a new dictionary with the smallest keys and their corresponding values
        smallest_dict = {key:self.feat[key] for key in smallest_keys}
        
        # Basically extract the station names to make life easy in the next function
        # This definitely could be improved
        
        filtered_dict = {key: value for key, value in names.items()  if key in list(smallest_dict.keys())}
        # #print(filtered_dict)
        return smallest_dict, filtered_dict

    def compare_smallest(self):
        all_dict = self.get_wrf_xy()
        dict, filtered_dict = self.read_csv()
        allkeys = np.array(list(dict.keys()))
        fig, axes = plt.subplots(2, 3, figsize=(15, 10))
                
        for index, key in enumerate(allkeys):
            ax = axes[index // 3, index % 3]
            row, col = divmod(index, 3)
            name = f'df_{key}.csv'
            path = self.path_to_csv+'/'+name
            generic = f'{filtered_dict[key]} ({key}) Precipitation Accumulation (in) Start of Day Values'
            df = pd.read_csv(path)
            df = df[(df['Date'] >=self.start) & (df['Date'] <= self.end)]
            date_range = pd.date_range(self.start, self.end, freq='1D')
            df_filtered = df[generic].tolist()
            df_filtered = [value * 25.4 for value in df_filtered]
            ixlat,ixlon = all_dict[str(key)]
            wrf_precip = self.extract_precip(ixlat,ixlon)[2:-1] # should be adjust according to the input data
            ax.plot(date_range,df_filtered,'r--', label=f'snotel')
            ax.plot(date_range,wrf_precip, label=f'WSM6')
            ax.set_title(f'{filtered_dict[key]} ({key})')
            ax.legend()
            
            if row == 1:  # Only for the bottom row
                ax.set_xlabel('Day')
            else:
                ax.set_xlabel('')
                
            if col == 0:
                ax.set_ylabel('Precipitation (mm)')
            ax.legend()
        
                    # Set x-axis locator and formatter for exterior plots
            if row ==1:
                ax.xaxis.set_major_locator(mdates.WeekdayLocator())
                ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
                ax.tick_params(axis='x', rotation=45)
            else:
                ax.xaxis.set_major_locator(plt.NullLocator())  # Hide x-axis locator
                ax.set_xlabel('')  # Set x-label to empty string
            
        # Adjust layout and show plots
        plt.tight_layout()

        if self.save:
            plt.savefig(self.save_name+'.pdf',dpi=600)
            
        plt.show()

if __name__ == '__main__':
    x = getclosest('NRCS_SNOTEL_Locations_noAlaska.csv','temp1',\
               '/bsuhome/stanleyakor/scratch/WY_22_THOMPSON/wrfout_d02_2021-09-29_00:00:00',\
               '/bsuhome/stanleyakor/Python-tutorial/wrf-python/short.nc')
    feat = x.compare_smallest()