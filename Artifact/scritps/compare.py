from packages import *
class precip_snotel():
    def __init__(self,var, path_to_header,path_to_csv,path_to_geog,path_to_wrf_file,save_name, save = True):
        self.path_to_header = path_to_header
        self.path_to_csv = path_to_csv
        self.geog = xr.open_dataset(path_to_geog)
        self.var = var
        self.dict = {}

        # self.start = '2021-10-01'
        # self.end = '2022-09-30'

        self.start = '2022-10-01'
        self.end = '2023-07-31'
        self.wrf = xr.open_dataset(path_to_wrf_file)
        self.wrf = self.wrf.sel(XTIME=slice(self.start, self.end))
        self.wrf_file = self.wrf[self.var]
        self.feat = {}
        self.save = save
        self.save_name = save_name

    def has_nan(self,lst):
        for item in lst:
            if isinstance(item, (float, int)) and math.isnan(item):
                return True
        return False

    def collect_snodas_info(self):
        df = pd.read_csv(self.path_to_header)
        df = df[(44.35894 >= df['Latitude']) & (df['Latitude'] >= 42.603153) &\
                (-113.64972 >= df['Longitude']) & (df['Longitude'] >= -116.31619) ] #& (df['Elevation']>=9000)

        filtered_df = df[df['State'] == 'ID']
        sta_names = filtered_df['Station Name'].tolist()
        lat = filtered_df['Latitude'].tolist()
        lon = filtered_df['Longitude'].tolist()
        sta_id = filtered_df['Station ID'].tolist()
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
        
        best = True
        worse = False
        
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
            
            # Fill up nan values with preceding values (somehow, snotel data has some empty values!)
            if self.has_nan(df_filtered):
                for i in range(1, len(df_filtered)):
                    if math.isnan(df_filtered[i]):
                        df_filtered[i] = df_filtered[i - 1] #0
                                    
            ixlat,ixlon = dict[str(sta_id[id])]
            wrf_precip = self.extract_precip(ixlat,ixlon)
            wrf_precip = wrf_precip
            bias = (df_filtered - wrf_precip).mean()
            self.feat[sta_id[id]] = bias
            names[sta_id[id]]=sta_names[id]

        # Get the keys with the smallest values (best 6)
        if best:
            smallest_keys = sorted(self.feat, key=lambda k: abs(self.feat[k]))[:6]

        # Get largest bias snotel guages (worse 6 guages)
        if worse:
            smallest_keys = sorted(self.feat, key=lambda k: abs(self.feat[k]))[-6:]
        # Create a new dictionary with the smallest keys and their corresponding values
        smallest_dict = {key:self.feat[key] for key in smallest_keys}
        
        # Basically extract the station names to make life easy in the next function
        # This definitely could be improved
        
        filtered_dict = {key: value for key, value in names.items()  if key in list(smallest_dict.keys())}
        # #print(filtered_dict)
        return smallest_dict, filtered_dict

    def read_csv2(self):
        
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
            wrf_precip = wrf_precip
            bias = (df_filtered - wrf_precip).mean()
            self.feat[sta_id[id]] = bias
            names[sta_id[id]]=sta_names[id]

        
        smallest_keys = sorted(self.feat, key=lambda k: abs(self.feat[k]))
       
        # Create a new dictionary with the smallest keys and their corresponding values
        smallest_dict = {key:self.feat[key] for key in smallest_keys}
        
        # # Basically extract the station names to make life easy in the next function
        # # This definitely could be improved
        
        filtered_dict = {key: value for key, value in names.items()  if key in list(smallest_dict.keys())}
        # # #print(filtered_dict)
        return smallest_dict, filtered_dict

    def compare_smallest(self):
        all_dict = self.get_wrf_xy()
        dict, filtered_dict = self.read_csv()
        allkeys = np.array(list(dict.keys()))
        fig, axes = plt.subplots(2, 3, figsize=(10, 6))
                
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

            # Fill up nan values with preceding values (somehow, snotel data has some empty values!)
            if self.has_nan(df_filtered):
                for i in range(1, len(df_filtered)):
                    if math.isnan(df_filtered[i]):
                            df_filtered[i] = df_filtered[i - 1] #0
            
            ixlat,ixlon = all_dict[str(key)]
            wrf_precip = self.extract_precip(ixlat,ixlon)
            ax.plot(date_range,df_filtered,'r--', label=f'snotel')
            ax.plot(date_range,wrf_precip, label=f'WSM6')
            ax.set_title(f'{filtered_dict[key]} ({key})')
            
            if row == 1:  # Only for the bottom row
                pass
                #ax.set_xlabel('Day')
            else:
                ax.set_xlabel('')
                
            if col == 0:
                ax.set_ylabel('Precipitation (mm)')

            if index == 0:
                ax.legend()
        
                    # Set x-axis locator and formatter for exterior plots
            if row ==1:
                ax.xaxis.set_major_locator(mdates.MonthLocator())
                ax.xaxis.set_major_formatter(mdates.DateFormatter('%b'))
                #ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
                ax.tick_params(axis='x', rotation=45)
            else:
                ax.xaxis.set_major_locator(plt.NullLocator())  # Hide x-axis locator
                ax.set_xlabel('')  # Set x-label to empty string
            
        # Adjust layout and show plots
        plt.tight_layout()

        if self.save:
            plt.savefig(self.save_name+'.pdf',dpi=600)
            
        plt.show()


class CompareScheme(precip_snotel):  
    def __init__(self,var, path_to_header, path_to_csv, path_to_geog, path_to_wrf_file, save_name, reference, save = True):
        super().__init__(var,path_to_header, path_to_csv, path_to_geog, path_to_wrf_file, save_name,save)
        self.allfile = {}
        self.check = False

        '''
        Initialize the first file, this certainly should be improved!
        
        '''
        self.allfile[reference] = self.wrf
    def compare_multiple(self,diction):
        for key,value in diction.items():
            self.wrf = xr.open_dataset(value)
            self.wrf= self.wrf.sel(XTIME=slice(self.start,self.end))
            self.allfile[key] = self.wrf
        return self.allfile

    # read multiple wrf files (schemes)
    def read_multiple(self,diction):
        all_file = self.compare_multiple(diction)
        all_files_dict = {}
        for key , file in all_file.items():
            self.wrf_file = file[self.var]
            smallest_dict, filtered_dict = self.read_csv()
            all_files_dict[key] = smallest_dict,filtered_dict
        return all_files_dict

    def extract(self,file,ixlat,ixlon):
        return file.isel(south_north = ixlat, west_east = ixlon).values
        
    def smallest(self,diction):
        all_dict = self.get_wrf_xy()
        allfiles = self.read_multiple(diction)
        schemes = list(allfiles.keys())
        wrf_files = self.compare_multiple(diction)

        # Since all the sites are the same for all schemes
        # Extract sites ID for only one of them
        filtered_dict = next(iter(allfiles.items()))[1][1]
        filtered_dict = dict(sorted(filtered_dict.items()))
        allkeys = np.array(list(filtered_dict.keys()))
        
        #print(filtered_dict)
        fig, axes = plt.subplots(2, 3, figsize=(10, 6))
                
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

            # Fill up nan values with preceding values (somehow, snotel data has some empty values!)
            if self.has_nan(df_filtered):
                for i in range(1, len(df_filtered)):
                    if math.isnan(df_filtered[i]):
                        df_filtered[i] = df_filtered[i - 1] #0
            
            ixlat,ixlon = all_dict[str(key)]
            ax.plot(date_range,df_filtered,'r--', label=f'snotel')

            for key1,value in wrf_files.items():
                
                wrf_precip = self.extract(value[self.var],ixlat,ixlon)
                ax.plot(date_range,wrf_precip, label=f'{key1}')

            ax.set_title(f'{filtered_dict[key]} ({key})')

            if index == 0:
                ax.legend()
            
            if row == 1:  # Only for the bottom row
                pass
                #ax.set_xlabel('Day')
            else:
                ax.set_xlabel('')
                
            if col == 0:
                ax.set_ylabel('Precipitation (mm)')
        
                    # Set x-axis locator and formatter for exterior plots
            if row ==1:
                ax.xaxis.set_major_locator(mdates.MonthLocator())
                ax.xaxis.set_major_formatter(mdates.DateFormatter('%b'))
                #ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
                ax.tick_params(axis='x', rotation=45)
            else:
                ax.xaxis.set_major_locator(plt.NullLocator())  # Hide x-axis locator
                ax.set_xlabel('')  # Set x-label to empty string
            
        # Adjust layout and show plots
        plt.tight_layout()

        if self.save:
             plt.savefig(self.save_name+'.pdf',dpi=600)
        
        plt.show()
            
            
'''
Extract show height and SWE from snotel sites and compare with snodas/wrf on peak day!

'''

class hist(CompareScheme):
    def __init__(self,var, path_to_header, path_to_csv, path_to_geog, path_to_wrf_file,\
                 save_name,reference,snodas_regrid_file, case, max_accum_date='2022-01-12', save=True):
        super().__init__(var,path_to_header, path_to_csv, path_to_geog, path_to_wrf_file, save_name,reference,save)

        self.max_accum_date = max_accum_date
        self.case = case
        self.snodas = snodas_regrid_file
        self.hist_snotel = {}
        self.hist_snodas = {}
        self.hist_Wdm6 = {}
        self.hist_Wsm6 = {}
        self.hist_Thom = {}
        self.hist_Mor = {}
        self.ttime = max_accum_date #184

    def make(self,diction):
        self.allfile = self.compare_multiple(diction)
        
        if self.case == 'snowh': #snow height
            xy = 'Snow Depth'
            var = 'SNOWH'
            
        if self.case == 'swe':   #Snow water equivalent
            xy = 'Snow Water Equivalent'
            var = 'SNOW'
            
        a, b = self.read_csv2()
        all_dict = self.get_wrf_xy() #convert station lat/lon to wrf/snodas index
             #read from snotel
        for key,value in b.items():
            name = f'df_{key}.csv'
            path = self.path_to_csv+'/'+name
            generic = f'{value} ({key}) {xy} (in) Start of Day Values'
            df = pd.read_csv(path)
            df_value = df.loc[df['Date'] == self.max_accum_date, generic].iloc[0]
    
                # Convert the value from inches to millimeters
            self.hist_snotel[str(key)] = df_value* 25.4

            #print(self.hist_snotel)
            #read snodas snow height values;
        for key, value in all_dict.items():
            ixlat,ixlon = all_dict[str(key)]
            snodas_value = self.extract(self.snodas,ixlat,ixlon)
            self.hist_snodas[str(key)] = snodas_value.astype(float)

            #print(self.hist_snodas)

            #Get values from the various schemes
        for key,value in self.allfile.items():
            if key == 'WSM6':
                for ID, name in all_dict.items():
                    ixlat,ixlon = all_dict[str(ID)]
                    wrf = value[var].sel(XTIME = self.ttime) #change so it is done automatically!
                    
                    if self.case == 'swe': 
                        wrf_value = self.extract(wrf,ixlat,ixlon)
                    if self.case == 'snowh':
                        wrf_value = self.extract(wrf,ixlat,ixlon)*1e3
                         
                    self.hist_Wsm6[ID] = wrf_value

            if key == 'WDM6':
                for ID, name in all_dict.items():
                    ixlat,ixlon = all_dict[str(ID)]
                    wrf = value[var].sel(XTIME = self.ttime) #change so it is done automatically!
                    
                    if self.case == 'swe': 
                        wrf_value = self.extract(wrf,ixlat,ixlon)
                    if self.case == 'snowh':
                        wrf_value = self.extract(wrf,ixlat,ixlon)*1e3
                         
                    self.hist_Wdm6[ID] = wrf_value
                        
            if key == 'Morrison':
                for ID, name in all_dict.items():
                    ixlat,ixlon = all_dict[str(ID)]
                    wrf = value[var].sel(XTIME = self.ttime) #change so it is done automatically!
                    
                    if self.case == 'swe': 
                        wrf_value = self.extract(wrf,ixlat,ixlon)
                    if self.case == 'snowh':
                        wrf_value = self.extract(wrf,ixlat,ixlon)*1e3
                         
                    self.hist_Mor[ID] = wrf_value

            if key == 'Thompson':
                for ID, name in all_dict.items():
                    ixlat,ixlon = all_dict[str(ID)]
                    wrf = value[var].sel(XTIME = self.ttime) #change so it is done automatically!
                    
                    if self.case == 'swe': 
                        wrf_value = self.extract(wrf,ixlat,ixlon)
                    if self.case == 'snowh':
                        wrf_value = self.extract(wrf,ixlat,ixlon)*1e3
                         
                    self.hist_Thom[ID] = wrf_value

        return self.hist_snodas, self.hist_snotel, self.hist_Wsm6, self.hist_Wdm6, self.hist_Mor,self.hist_Thom



    def make_plots(self, diction,title):
        self.hist_snodas, self.hist_snotel, self.hist_Wsm6, self.hist_Wdm6, self.hist_Mor, self.hist_Thom = self.make(diction)
        a, sta_names = self.read_csv2()

        # remove the above keys from self.hist_snodas, self.hist_snotel, self.hist_Wsm6, self.hist_Wdm6, self.hist_Mor

        keys_to_remove = ['496', '704']

        # Create new dictionaries without the unwanted keys
        self.hist_snodas = {key: value for key, value in self.hist_snodas.items() if key not in keys_to_remove}
        self.hist_snotel = {key: value for key, value in self.hist_snotel.items() if key not in keys_to_remove}
        self.hist_Wsm6 = {key: value for key, value in self.hist_Wsm6.items() if key not in keys_to_remove}
        self.hist_Wdm6 = {key: value for key, value in self.hist_Wdm6.items() if key not in keys_to_remove}
        self.hist_Mor = {key: value for key, value in self.hist_Mor.items() if key not in keys_to_remove}
        self.hist_Thom = {key: value for key, value in self.hist_Thom.items() if key not in keys_to_remove}
        sta_names = {key: value for key, value in sta_names.items() if str(key) not in keys_to_remove}
    
        fig, axes = plt.subplots(7, 3, figsize=(12, 25))  # Adjust figsize as needed
    
        non_empty_figure_count = 0
        max_figures = 21
    
        for index, key in enumerate(sta_names.keys()):
            key = str(key)
            if non_empty_figure_count < max_figures:
                row, col = divmod(non_empty_figure_count, 3)  # Adjust the division here
                ax = axes[row, col]
                data = {'Snodas': self.hist_snodas[key],
                        'Snotel': self.hist_snotel[key],
                        'WSM6': self.hist_Wsm6[key],
                        'WDM6': self.hist_Wdm6[key],
                        'Thompson': self.hist_Thom[key],
                        'MOR': self.hist_Mor[key]}
    
                keys = list(data.keys())
                values = list(data.values())
    
                # Specify different colors for the bars
                colors = ['blue', 'orange', 'green', 'red', 'purple', 'black']
    
                # Create a bar plot with specified colors
                bars = ax.bar(keys, values, color=colors)
    
                ax.set_title(f'{sta_names[int(key)]} ({key})')
    
                if row == 6:  # Add x-axis labels only for the last row
                    ax.set_xticks(keys)
                    ax.set_xticklabels(keys, rotation=45)
                else:
                    ax.set_xticks([])  # Remove x-axis ticks and labels for other rows
    
                if col == 0:  # Add y-axis labels only for the first column
                    ax.set_ylabel(title)
    
                non_empty_figure_count += 1

        if self.save:
            plt.savefig(self.save_name+'.pdf',dpi=600)

'''
Compare correlation coefficient (wrf and snotel)
across the various microphysics schemes

'''


class station_corr(CompareScheme):
    def __init__(self,var, path_to_header, path_to_csv, path_to_geog, path_to_wrf_file, save_name, reference, save = True):
        super().__init__(var, path_to_header, path_to_csv, path_to_geog, path_to_wrf_file, save_name, reference, save)

        self.var_list = ['PRCP', 'SNOW', 'SNOWH']

    def has_nan(self,lst):
        for item in lst:
            if isinstance(item, (float, int)) and math.isnan(item):
                return True
        return False

    def compute_NSE(self,df,wrf):
        numerator = np.sum((df - wrf) ** 2)
        denominator = np.sum((df - np.mean(df)) ** 2)
        NSE = 1 - (numerator / denominator)
        return NSE
    
    def compute_KGE(self, df,wrf):
        r, _ = pearsonr(df, wrf)

        # Calculate alpha and beta
        alpha = np.std(wrf) / np.std(df)
        beta = np.mean(wrf) / np.mean(df)

        # Calculate KGE
        kge = 1 - np.sqrt((r - 1)**2 + (alpha - 1)**2 + (beta - 1)**2)
        return kge
        
    def worker(self,diction,scheme, type = 'NSE'):
        site_name, precip_cor, snowh_cor, swe_cor, precip_spear, snowh_spear, swe_spear = [], [], [], [], [], [], []
        # why does sthe dictionary key get renamed from WSM6 to true?
        # if scheme == 'WSM6':
        #     scheme = True  
            
        all_dict = self.get_wrf_xy()
        allfiles = self.compare_multiple(diction)
        a, b = self.read_csv2()

        scheme_files = allfiles[scheme]
        #Extract variable from snotel csv and wrf
        var_to_fname = {
                        'PRCP': 'Precipitation',
                    'SNOW': 'Snow Water Equivalent',
                       'SNOWH': 'Snow Depth'
                        } 
        
        # desired_keys ={704,423,496,769,637,978,312,550,489,306,830,450,490,845} #{537} #
        
        # # Creating a new dictionary with only the desired keys
        # a = {key:a[key] for key in a if key in desired_keys}
        # b = {key:b[key] for key in b if key in desired_keys}

        
        for sta_id,sta_name,  in b.items():
            site_name.append(f'{sta_name} ({sta_id})')
            for var in self.var_list:
                ixlat,ixlon = all_dict[str(sta_id)]
                fname = var_to_fname.get(var, 'Unknown Variable')
                generic = f'{sta_name} ({sta_id}) {fname} (in) Start of Day Values'
                if var == 'PRCP':
                    generic =f'{sta_name} ({sta_id}) {fname} Accumulation (in) Start of Day Values' 
                name = f'df_{sta_id}.csv'
                path = self.path_to_csv+'/'+name
                df = pd.read_csv(path)
                df = df[(df['Date'] >=self.start) & (df['Date'] <= self.end)]
                df_filtered = df[generic].tolist()
                df_filtered = [value * 25.4 for value in df_filtered]
               
                # Fill up nan values with preceding values (somehow, snotel data has some empty values!)
                if self.has_nan(df_filtered):
                        for i in range(1, len(df_filtered)):
                            if math.isnan(df_filtered[i]):
                                df_filtered[i] = 0 #df_filtered[i - 1]
    
                if var == 'PRCP':
                    wrf= self.extract(scheme_files['PRCP'],ixlat,ixlon)# should be adjust according to the input data
                    # #compute correlation
                    if type == 'KGE':
                        precip_cor.append(self.compute_KGE(df_filtered,wrf))

                    if type == 'NSE':
            
                        precip_cor.append(self.compute_NSE(df_filtered,wrf))
    
                if var == 'SNOW':
                    wrf= self.extract(scheme_files['SNOW'],ixlat,ixlon) # should be adjust according to the input data
                    # #compute correlation

                    if type == 'KGE':
                        swe_cor.append(self.compute_KGE(df_filtered,wrf))

                    if type == 'NSE':
                        
                        swe_cor.append(self.compute_NSE(df_filtered,wrf))         
                

                if var == 'SNOWH':
                    wrf= self.extract(scheme_files['SNOWH'],ixlat,ixlon)*1e3  # should be adjust according to the input data
                    # #compute correlation

                    if type == 'KGE':
                        snowh_cor.append(self.compute_KGE(df_filtered,wrf))

                    if type == 'NSE':
                        snowh_cor.append(self.compute_NSE(df_filtered,wrf))
        
        return pd.DataFrame({'Name': site_name,
                             'PRCP_'+type: precip_cor,
                             'SNOWH_'+type: snowh_cor,
                             'SWE_'+type: swe_cor})

    def make(self, diction, type):
        #return self.worker(diction,'WSM6', type)
        wsm6_cfs_df = self.worker(diction,'WSM6-CFS', type)  
        wdm6_cfs_df = self.worker(diction,'WDM6-CFS', type)
        th_cfs_df = self.worker(diction,'Thompson-CFS', type)   
        mor_cfs_df = self.worker(diction,'Morrison-CFS', type)

        wsm6_era_df = self.worker(diction,'WSM6-ERA', type)
        wdm6_era_df = self.worker(diction,'WDM6-ERA', type)
        th_era_df = self.worker(diction,'Thompson-ERA', type)
        mor_era_df = self.worker(diction,'Morrison-ERA', type)

        # listt = [wsm6_df,wdm6_df, th_df,  mor_df ]

        precip_correlation = pd.DataFrame({'Name' : wsm6_cfs_df['Name'],
                                            'WS-CFS' : wsm6_cfs_df['PRCP_'+type],
                                            'WD-CFS' : wdm6_cfs_df['PRCP_'+type],
                                            'TH-CFS' : th_cfs_df['PRCP_'+type],
                                            'MOR-CFS' : mor_cfs_df['PRCP_'+type],
                                            'WS-ERA' : wsm6_era_df['PRCP_'+type],
                                            'WD-ERA': wdm6_era_df['PRCP_'+type],
                                            'TH-ERA' : th_era_df['PRCP_'+type],
                                            'MOR-ERA' : mor_era_df['PRCP_'+type],
                                            })
        precip_correlation.set_index('Name', inplace=True)

        ## swe corr dataframe
        swe_correlation = pd.DataFrame({'Name' : wsm6_cfs_df['Name'],
                                            'WS-CFS' : wsm6_cfs_df['SWE_'+type],
                                            'WD-CFS' : wdm6_cfs_df['SWE_'+type],
                                            'TH-CFS' : th_cfs_df['SWE_'+type],
                                            'MOR-CFS' : mor_cfs_df['SWE_'+type],
                                            'WS-ERA' : wsm6_era_df['SWE_'+type],
                                            'WD-ERA' : wdm6_era_df['SWE_'+type],
                                            'TH-ERA': th_era_df['SWE_'+type],
                                            'MOR-ERA': mor_era_df['SWE_'+type],
                                            })
        swe_correlation.set_index('Name', inplace=True)

        # snowh corr dataframe
        swh_correlation = pd.DataFrame({'Name' : wsm6_cfs_df['Name'],
                                            'WS-CFS': wsm6_cfs_df['SNOWH_'+type],
                                            'WD-CFS': wdm6_cfs_df['SNOWH_'+type],
                                            'TH-CFS' : th_cfs_df['SNOWH_'+type],
                                             'MOR-CFS' : mor_cfs_df['SNOWH_'+type],
                                           'WS-ERA': wsm6_era_df['SNOWH_'+type],
                                               'WD-ERA': wdm6_era_df['SNOWH_'+type],
                                              'TH-ERA' : th_era_df['SNOWH_'+type],
                                            'MOR-ERA' : mor_era_df['SNOWH_'+type],
                                            })
        swh_correlation.set_index('Name', inplace=True)

        # print(f" Precipitation {type} coefficient across microphysics schemes")
        # print(precip_correlation)
        # print("  ")

        # print(" SWE {type} coefficient across microphysics schemes")
        # print(swe_correlation)
        # print("  ")
        
        # print(" snow height {type} coefficient across microphysics schemes")
        # print(swh_correlation)

        precip_correlation.to_csv('precip_station_data.csv')
        swe_correlation.to_csv('swe_station_data.csv')
        swh_correlation.to_csv('swh_station_data.csv')

# take the mean snow height across all the snotel sites per day and compare this with the mean across the 
# microphysics scheme, make a plot of each scheme with snotel


'''

        Plot snotel kge score vs elevation and compute correlation statistics

'''


def plot_KGE(swe,elevation,tyype = 'KGE'):
    merged = pd.merge(swe, elevation, on='Name', how='inner')

    listt = ['WSM6', 'WDM6', 'Thompson', 'Morrison']

    plt.figure(figsize=(8,6))
    fig, axes = plt.subplots(2, 2, figsize=(10, 8))

    for i, df_name in enumerate(listt):
        name = f'{df_name}_{tyype}'
        ax = axes[i // 2, i % 2]  # Get the current axis using integer division and modulo
        ax.scatter(merged[name], merged['Elevation'], color='green')  # Access merged DataFrame columns
        correlation_coefficient = np.corrcoef(merged[name], merged['Elevation'])[0, 1]
        
        ax.text(0.05, 0.95, f'$\\rho$: {correlation_coefficient:.2f}', fontsize=8, verticalalignment='top', horizontalalignment='left', transform=ax.transAxes)
        
        # Fit a linear regression model
        X = np.array(merged[name]).reshape(-1, 1)
        y = merged['Elevation']
        model = LinearRegression()
        model.fit(X, y)

        y_pred = model.predict(X)

        # Plot the regression line
        ax.plot(X, y_pred, color='red')

        ax.set_title(df_name)

        if i % 2 != 0:  # hide yticks for second plot column
                    
            ax.set_yticklabels([])
                    
        if i > 1:
            ax.set_xlabel(f'{tyype} score')

        if i%2==0:
            ax.set_ylabel('Height')

    # Adjust layout and show the plots
    plt.tight_layout()
    plt.show()



''''
Plot time series of snow height, precip or swe in a single snotel station across MP schemes
'''
class snotel_snowheight(CompareScheme):
    def __init__(self,var, path_to_header, path_to_csv, path_to_geog, path_to_wrf_file, save_name, reference, save = True):
        super().__init__(var, path_to_header, path_to_csv, path_to_geog, path_to_wrf_file, save_name, reference, save)
        self.var_list = ['SNOW']#, 'SNOW', 'SNOWH'
    

    def has_nan(self,lst):
        for item in lst:
            if isinstance(item, (float, int)) and math.isnan(item):
                return True
            return False

    def worker(self,diction,scheme):
       
        snotel_bucket,wrf_bucket = np.zeros(365),np.zeros(365)

        all_dict = self.get_wrf_xy()
        allfiles = self.compare_multiple(diction)
        a, b = self.read_csv2()

        scheme_files = allfiles[scheme]
        #Extract variable from snotel csv and wrf
        var_to_fname = {
                        'SNOW': 'Snow Water Equivalent',
                        } 
        # ,
        #             'SNOW': 'Snow Water Equivalent',
        #                'SNOWH': 'Snow Depth'
        mean_dict = {}

        # Check single sight

        sta_name = 'Prairie' # CHANGE ME!
        sta_id =  704
        b = {}
        b[sta_id] = sta_name


        num_site = len(b.keys())
        #print(num_site)
        for var in self.var_list:
            for sta_id,sta_name,  in b.items():
                #print(sta_name)
                ixlat,ixlon = all_dict[str(sta_id)]
                fname = var_to_fname.get(var, 'Unknown Variable')
                generic = f'{sta_name} ({sta_id}) {fname} (in) Start of Day Values'
                if var == 'PRCP':
                    generic =f'{sta_name} ({sta_id}) {fname} Accumulation (in) Start of Day Values' 
                name = f'df_{sta_id}.csv'
                path = self.path_to_csv+'/'+name
                df = pd.read_csv(path)
                df = df[(df['Date'] >=self.start) & (df['Date'] <= self.end)]
                df_filtered = df[generic].tolist()
                df_filtered = [value * 25.4 for value in df_filtered]

                '''
                    TROUBLE SHOOT!
                '''
                # meann = np.mean(df_filtered)
                # print(meann, sta_name, sta_id)

                snotel_bucket += df_filtered
                
               
                if var == 'PRCP':
                    wrf= self.extract(scheme_files['PRCP'],ixlat,ixlon)# should be adjust according to the input data
            
    
                if var == 'SNOW':
                    wrf= self.extract(scheme_files['SNOW'],ixlat,ixlon) # should be adjust according to the input data
                    # #compute correlation    
                

                if var == 'SNOWH':
                    wrf= self.extract(scheme_files['SNOWH'],ixlat,ixlon)*1e3  # should be adjust according to the input data
                    # #compute correlation

                wrf_bucket += wrf

            mean_dict[var] = (snotel_bucket/num_site,wrf/num_site)

        return mean_dict


## Times series of snow height across select snotel sites
class precip_temp_compare(CompareScheme):
    def __init__(self,var, path_to_header, path_to_csv, path_to_geog, path_to_wrf_file, save_name, reference, save = True):
        super().__init__(var, path_to_header, path_to_csv, path_to_geog, path_to_wrf_file, save_name, reference, save)
        self.var_list = ['SNOWH','T2']
        
    def to_kelvin(self, df):
        df = np.array(df)
        return (df - 32) * 5/9 + 273.15
        
    def smallest(self,diction):
        #Extract variable from snotel csv and wrf
        var_to_fname = {
                       'SNOWH': 'Snow Depth',
                         'T2' : 'Air Temperature Average (degF)'
                        } 

        
        all_dict = self.get_wrf_xy()
        allfiles = self.read_multiple(diction)
        schemes = list(allfiles.keys())
        wrf_files = self.compare_multiple(diction)
        a, b = self.read_csv2()

                # List of keys to keep
        desired_keys = {978, 637, 312,845}
        
        # Creating a new dictionary with only the desired keys
        a = {key:a[key] for key in a if key in desired_keys}
        b = {key:b[key] for key in b if key in desired_keys}
        
        date_range = pd.date_range(self.start, self.end, freq='1D')
        plt.figure(figsize=(16, 10))
        gs = gridspec.GridSpec(4, 2, height_ratios=[1, 4, 1, 4],hspace=0.15) 
        
        for i, (sta_id, sta_name) in enumerate(b.items()):

            if i > 1:
                i+=2
                
            for var in self.var_list:
                ixlat,ixlon = all_dict[str(sta_id)]
                fname = var_to_fname.get(var, 'Unknown Variable')
                if var == 'T2':
                    generic = f'{sta_name} ({sta_id}) {fname}'
                        
                else:
                    generic = f'{sta_name} ({sta_id}) {fname} (in) Start of Day Values'
                        
                name = f'df_{sta_id}.csv'
                path = self.path_to_csv+'/'+name
                df = pd.read_csv(path)
                df = df[(df['Date'] >=self.start) & (df['Date'] <= self.end)]
                df_filtered = df[generic].tolist()
                    
                if var == 'T2':
                    df_filtered_temp = self.to_kelvin(df_filtered)
                    df_filtered_temp = pd.Series(df_filtered_temp).rolling(7).mean()
                        
                else:
                    df_filtered_snowh = [value * 25.4 for value in df_filtered]
    
            ax1 = plt.subplot(gs[i])
            ax1.plot(df_filtered_temp,'--', label='Snotel')
            ax1.set_title(f'{sta_name} ({sta_id})')
            
            ax2 = plt.subplot(gs[i+2])
            ax2.plot(date_range,df_filtered_snowh,'--', label='Snotel')

            
            for key1,value in wrf_files.items():
                for var in self.var_list:
                    
                    if var == 'SNOWH':
                        wrf_snowh_height= self.extract(value['SNOWH'],ixlat,ixlon)*1e3  # should be adjust according to the input data
                    else:
                        wrf_temp2m= self.extract(value['T2'],ixlat,ixlon)
                        wrf_temp2m = pd.Series(wrf_temp2m).rolling(7).mean()
    
                                       
                ax1.plot(wrf_temp2m, label=f'{key1}')
                ax2.plot(date_range,wrf_snowh_height, label=f'{key1}')
                ax2.xaxis.set_major_locator(mdates.MonthLocator())
                ax2.xaxis.set_major_formatter(mdates.DateFormatter('%b'))
                #ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
                ax2.tick_params(axis='x', rotation=45)
                
            if i == 0 or i ==2 or i ==4:
                ax1.set_ylabel("Temp. (K)")
                ax2.set_ylabel("Snow height (mm)")
                
            if i ==0:
                ax2.legend(loc='upper left')
                
            if i < 4 :
                ax1.set_xticks([])
                ax2.set_xticks([])
            if  5==i<6 or i ==4:
                ax1.set_xticks([])

            if self.save:
                 plt.savefig(self.save_name+'.pdf',dpi=600)

## Average across time for all snotel and wrf sites
## Average across time for all snotel and wrf sites
class snotel_average(CompareScheme):
    def __init__(self,var, path_to_header, path_to_csv, path_to_geog, path_to_wrf_file, save_name, reference,save = True):
        super().__init__(var, path_to_header, path_to_csv, path_to_geog, path_to_wrf_file, save_name, reference, save)
        
        self.var_list = ['SNOWH']
        
    def to_kelvin(self, df):
        df = np.array(df)
        return (df - 32) * 5/9 + 273.15

    def has_nan(self,lst):
        for item in lst:
            if isinstance(item, (float, int)) and math.isnan(item):
                return True
        return False
        
    def compute(self,diction,type,ax,legend = False):

        if type is None:
            raise Exception('Please select a type: SNOW, SNOWH, or PRCP')
    
        if type == 'SNOWH':
            #Extract variable from snotel csv and wrf
            var_to_fname = {
                           'SNOWH': 'Snow Depth'
                            } 
            
        if type == 'SNOW':
            var_to_fname = {
                           'SNOW': 'Snow Water Equivalent'
                                } 
            
            self.var_list = ['SNOW']
                        
        if type == 'PRCP':
            var_to_fname = {
                           'PRCP': 'Precipitation Accumulation'
                            } 
            self.var_list = ['PRCP']

        if type == 'T2':
            var_to_fname = {'T2' : 'Air Temperature Average (degF)'}

            self.var_list = ['T2']

        def get_marker_and_color(key1):
            color_dict = {'E': 'green', 'R': 'blue', 'A': 'red'}
            
            if key1[0:2] == 'MO':
                marker = '--'
                if 'E' in key1:
                    color = color_dict['E']
                else:
                    color = color_dict['A']
            
            if key1[0:2] == 'TH':
                marker = ':'
                if 'E' in key1:
                    color = color_dict['E']
                else:
                    color = color_dict['A']
          
            if key1[0:2] == 'WD':
                marker = '-.'
                if 'E' in key1:
                    color = color_dict['E']
                else:
                    color = color_dict['A']
            if key1[0:2] == 'WS':
                marker = '-'
                if 'E' in key1:
                    color = color_dict['E']
                else:
                    color = color_dict['A']
        
            return marker, color
        # print(self.var_list)
        # print(var_to_fname)
        all_dict = self.get_wrf_xy()
        allfiles = self.read_multiple(diction)
        schemes = list(allfiles.keys())
        wrf_files = self.compare_multiple(diction)
        a, b = self.read_csv2()

                # List of keys to keep
        # desired_keys ={704,423,496,769,637,978,312,550,489,306,830,450,490,845} #{537} #
        
        # # Creating a new dictionary with only the desired keys
        # a = {key:a[key] for key in a if key in desired_keys}
        # b = {key:b[key] for key in b if key in desired_keys}
        
        date_range = pd.date_range(self.start, self.end, freq='1D')
        N = len(date_range)
        n = len(a)
        plt.figure(figsize = (12,8))

        colors = ['blue', 'red']
       

        for j, (key1, value) in enumerate(wrf_files.items()):
            array_swh = np.zeros(N)
            array_swh_snotel = np.zeros(N)
            for sta_id,sta_name,  in b.items():
                for var in self.var_list:
                    ixlat,ixlon = all_dict[str(sta_id)]
                    fname = var_to_fname.get(var, 'Unknown Variable')
                    
                    if var == 'T2':
                        generic = f'{sta_name} ({sta_id}) {fname}'
                        
                    else:
                        generic = f'{sta_name} ({sta_id}) {fname} (in) Start of Day Values'
                        
                    name = f'df_{sta_id}.csv'
                    path = self.path_to_csv+'/'+name
                    df = pd.read_csv(path)
                    df = df[(df['Date'] >=self.start) & (df['Date'] <= self.end)]
                    df_filtered = df[generic].tolist()
                    # Fill up nan values with preceding values (somehow, snotel data has some empty values!)
                    if self.has_nan(df_filtered):
                            for i in range(1, len(df_filtered)):
                                if math.isnan(df_filtered[i]):
                                    df_filtered[i] = df_filtered[i - 1] #0

                    if var != 'T2':
                        df_filtered= [value * 25.4 for value in df_filtered]
                        
                    else:
                        df_filtered = self.to_kelvin(df_filtered)
                    array_swh_snotel +=  df_filtered
             
                    wrf_snowh_height= self.extract(value[var],ixlat,ixlon) #*1e3  # should be adjust according to the input data

                    if var == 'SNOWH':
                        wrf_snowh_height *= 1e3
                    array_swh += wrf_snowh_height
                    
            marker, color = get_marker_and_color(key1)
            
            ax.plot(date_range,array_swh/n,marker,label=f'{key1}',color=color,linewidth=2)
            #plt.fill_between(date_range, array_swh/n-np.std(array_swh/n), array_swh/n+np.std(array_swh/n),alpha=0.1,color=colors[j])
        
        ax.plot(date_range,array_swh_snotel/n,'--', color='black',label=f'Snotel',linewidth=2)
        ax.fill_between(date_range, array_swh_snotel/n-np.std(array_swh_snotel/n), array_swh_snotel/n+np.std(array_swh_snotel/n),alpha=0.1,color='black')
        ax.xaxis.set_major_locator(mdates.MonthLocator())
        #ax.xaxis.set_major_formatter(mdates.DateFormatter('%b'))
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %Y'))
        ax.tick_params(axis='x', rotation=45)
        ax.set_ylim(np.max((0, np.min(array_swh_snotel/n-np.std(array_swh_snotel/n)))),np.max(array_swh_snotel/n+np.std(array_swh_snotel/n)))
        ax.set_xlim(date_range[0], date_range[-1])

        if legend:
            ax.legend()

        if type == 'SNOW':
            ax.set_ylabel('Snow water equivalent (mm)')
        if type ==  'SNOWH':
            ax.set_ylabel('Snow Depth (mm)')

        if type == 'PRCP':
            ax.set_ylabel('Accumulated precipitation (mm)')

        if type == 'T2':
            ax.set_ylabel('Temperature (K)')

        # if self.save:
        #     ax.savefig(self.save_name+'.pdf',dpi=600)


class snotel_average_diagnostics(CompareScheme):
    def __init__(self,var, path_to_header, path_to_csv, path_to_geog, path_to_wrf_file, save_name, reference,snodas, save = True):
        super().__init__(var, path_to_header, path_to_csv, path_to_geog, path_to_wrf_file, save_name, reference, save)
        
        self.var_list = ['SNOWH']
        self.snodas = snodas
        
    def to_kelvin(self, df):
        df = np.array(df)
        return (df - 32) * 5/9 + 273.15

    def has_nan(self,lst):
        for item in lst:
            if isinstance(item, (float, int)) and math.isnan(item):
                return True
        return False
        
    def compute(self,diction,type,legend = False):

        if type is None:
            raise Exception('Please select a type: SNOW, SNOWH, or PRCP')
    
        if type == 'SNOWH':
            #Extract variable from snotel csv and wrf
            var_to_fname = {
                           'SNOWH': 'Snow Depth'
                            } 
            
        if type == 'SNOW':
            var_to_fname = {
                           'SNOW': 'Snow Water Equivalent'
                                } 
            
            self.var_list = ['SNOW']
                        
        if type == 'PRCP':
            var_to_fname = {
                           'PRCP': 'Precipitation Accumulation'
                            } 
            self.var_list = ['PRCP']

        if type == 'T2':
            var_to_fname = {'T2' : 'Air Temperature Average (degF)'}

            self.var_list = ['T2']
            
        # print(self.var_list)
        # print(var_to_fname)
        all_dict = self.get_wrf_xy()
        allfiles = self.read_multiple(diction)
        schemes = list(allfiles.keys())
        wrf_files = self.compare_multiple(diction)
        a, b = self.read_csv2()

                # List of keys to keep
        # desired_keys ={704,423,496,769,637,978,312,550,489,306,830,450,490,845} #{537} #
        
        # # Creating a new dictionary with only the desired keys
        # a = {key:a[key] for key in a if key in desired_keys}
        # b = {key:b[key] for key in b if key in desired_keys}
        
        date_range = pd.date_range(self.start, self.end, freq='1D')
        N = len(date_range)
        n = len(b)
        plt.figure(figsize = (12,8))

        colors = ['blue', 'orange', 'green', 'red', 'purple'] 


        for j, (key1, value) in enumerate(wrf_files.items()):
            array_swh = np.zeros(N)
            array_swh_snotel = np.zeros(N)
            snodas_array = np.zeros(N)
            for sta_id,sta_name,  in b.items():
                for var in self.var_list:
                    ixlat,ixlon = all_dict[str(sta_id)]
                    fname = var_to_fname.get(var, 'Unknown Variable')
                    
                    if var == 'T2':
                        generic = f'{sta_name} ({sta_id}) {fname}'
                        
                    else:
                        generic = f'{sta_name} ({sta_id}) {fname} (in) Start of Day Values'
                        
                    name = f'df_{sta_id}.csv'
                    path = self.path_to_csv+'/'+name
                    df = pd.read_csv(path)
                    df = df[(df['Date'] >=self.start) & (df['Date'] <= self.end)]
                    df_filtered = df[generic].tolist()
                    # Fill up nan values with preceding values (somehow, snotel data has some empty values!)
                    if self.has_nan(df_filtered):
                            for i in range(1, len(df_filtered)):
                                if math.isnan(df_filtered[i]):
                                    df_filtered[i] = df_filtered[i - 1] #0

                    if var != 'T2':
                        df_filtered= [value * 25.4 for value in df_filtered]
                        
                    else:
                        df_filtered = self.to_kelvin(df_filtered)
                    array_swh_snotel +=  df_filtered
             
                    wrf_snowh_height= self.extract(value[var],ixlat,ixlon) #*1e3  # should be adjust according to the input data
                    snodas =  self.extract(self.snodas,ixlat,ixlon)

                    if var == 'SNOWH':
                        wrf_snowh_height *= 1e3
                    array_swh += wrf_snowh_height
                    snodas_array += snodas
            
        

            plt.plot(date_range,array_swh/n,label=f'{key1}',color=colors[j])
            #plt.fill_between(date_range, array_swh/n-np.std(array_swh/n), array_swh/n+np.std(array_swh/n),alpha=0.1,color=colors[j])
        
        if var != 'PRCP':
            plt.plot(date_range,snodas_array/n,label=f'Snodas',color='cyan',linestyle='--')

        plt.plot(date_range,array_swh_snotel/n,'--', color='black',label=f'Snotel')
        plt.fill_between(date_range, array_swh_snotel/n-np.std(array_swh_snotel/n), array_swh_snotel/n+np.std(array_swh_snotel/n),alpha=0.1,color='black')
        plt.gca().xaxis.set_major_locator(mdates.MonthLocator())
        plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%b'))
        plt.gca().tick_params(axis='x', rotation=45)
        plt.ylim(np.max((0, np.min(array_swh_snotel/n-np.std(array_swh_snotel/n)))),np.max(array_swh_snotel/n+np.std(array_swh_snotel/n)))
        plt.xlim(date_range[0], date_range[-1])

        if legend:
            plt.legend()

        if type == 'SNOW':
            plt.ylabel('Snow water equivalent (mm)')
        if type ==  'SNOWH':
            plt.ylabel('Snow height (mm)')

        if type == 'PRCP':
            plt.ylabel('Accumulated precipitation (mm)')

        if type == 'T2':
            plt.ylabel('Temperature (K)')

        if self.save:
            plt.savefig(self.save_name+'.pdf',dpi=600)