from packages import *
class precip_snotel():
    def __init__(self,var, path_to_header,path_to_csv,path_to_geog,path_to_wrf_file,save_name, save = True):
        self.path_to_header = path_to_header
        self.path_to_csv = path_to_csv
        self.geog = xr.open_dataset(path_to_geog)
        self.var = var
        self.dict = {}
        self.start = '2021-10-01'
        self.end = '2022-09-30'
        self.wrf = xr.open_dataset(path_to_wrf_file)
        self.wrf = self.wrf.sel(XTIME=slice(self.start, self.end))
        self.wrf_file = self.wrf[self.var]
        self.feat = {}
        self.save = save
        self.save_name = save_name

    def collect_snodas_info(self):
        df = pd.read_csv(self.path_to_header)
        df = df[(44.35894 >= df['Latitude']) & (df['Latitude'] >= 42.603153) &\
                (-113.64972 >= df['Longitude']) & (df['Longitude'] >= -116.31619)]

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

           
        # Get the keys from the first sub-dictionary
        keys_to_check = set(all_files_dict[next(iter(all_files_dict))][0].keys())
        #print(keys_to_check)
        
        # Iterate through the rest of the sub-dictionaries
        for key in all_files_dict:
            sub_dict_keys = all_files_dict[key][0].keys()
            if keys_to_check != set(sub_dict_keys):
                print(f"Keys in sub-dictionary '{key}' do not match.")
                break
        else:
            print("Keys in all sub-dictionaries match.")


        # If all schemes have best estimate at the same time, proceed
        #if self.check:
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
            ixlat,ixlon = all_dict[str(key)]
            #print(ixlat,ixlon)
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
                 save_name,reference,snodas_regrid_file, case, max_accum_date='2022-04-01', save=True):
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
        self.ttime = 184

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
                    wrf = value[var].isel(XTIME = self.ttime) #change so it is done automatically!
                    
                    if self.case == 'swe': 
                        wrf_value = self.extract(wrf,ixlat,ixlon)
                    if self.case == 'snowh':
                        wrf_value = self.extract(wrf,ixlat,ixlon)*1e3
                         
                    self.hist_Wsm6[ID] = wrf_value

            if key == 'WDM6':
                for ID, name in all_dict.items():
                    ixlat,ixlon = all_dict[str(ID)]
                    wrf = value[var].isel(XTIME = self.ttime) #change so it is done automatically!
                    
                    if self.case == 'swe': 
                        wrf_value = self.extract(wrf,ixlat,ixlon)
                    if self.case == 'snowh':
                        wrf_value = self.extract(wrf,ixlat,ixlon)*1e3
                         
                    self.hist_Wdm6[ID] = wrf_value
                        
            if key == 'Morrison':
                for ID, name in all_dict.items():
                    ixlat,ixlon = all_dict[str(ID)]
                    wrf = value[var].isel(XTIME = self.ttime) #change so it is done automatically!
                    
                    if self.case == 'swe': 
                        wrf_value = self.extract(wrf,ixlat,ixlon)
                    if self.case == 'snowh':
                        wrf_value = self.extract(wrf,ixlat,ixlon)*1e3
                         
                    self.hist_Mor[ID] = wrf_value

            if key == 'Thompson':
                for ID, name in all_dict.items():
                    ixlat,ixlon = all_dict[str(ID)]
                    wrf = value[var].isel(XTIME = self.ttime) #change so it is done automatically!
                    
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
                                df_filtered[i] = df_filtered[i - 1]
    
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
        wsm6_df = self.worker(diction,'WSM6', type)
        wdm6_df = self.worker(diction,'WDM6', type)
        th_df = self.worker(diction,'Thompson', type)
        mor_df = self.worker(diction,'Morrison', type)
        listt = [wsm6_df,wdm6_df, th_df,  mor_df ]

        precip_correlation = pd.DataFrame({'Name' : wsm6_df['Name'],
                                            'WSM6_'+type : wsm6_df['PRCP_'+type],
                                            'WDM6_'+type : wdm6_df['PRCP_'+type],
                                            'Thompson_'+type : th_df['PRCP_'+type],
                                            'Morrison_'+type : mor_df['PRCP_'+type]
                                            })
        precip_correlation.set_index('Name', inplace=True)

        ## swe corr dataframe
        swe_correlation = pd.DataFrame({'Name' : wsm6_df['Name'],
                                            'WSM6_'+type : wsm6_df['SWE_'+type],
                                            'WDM6_'+type : wdm6_df['SWE_'+type],
                                            'Thompson_'+type : th_df['SWE_'+type],
                                            'Morrison_'+type : mor_df['SWE_'+type]
                                            })
        swe_correlation.set_index('Name', inplace=True)

        # snowh corr dataframe
        swh_correlation = pd.DataFrame({'Name' : wsm6_df['Name'],
                                            'WSM6_'+type : wsm6_df['SNOWH_'+type],
                                            'WDM6_'+type : wdm6_df['SNOWH_'+type],
                                            'Thompson_'+type : th_df['SNOWH_'+type],
                                            'Morrison_'+type : mor_df['SNOWH_'+type]
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

    