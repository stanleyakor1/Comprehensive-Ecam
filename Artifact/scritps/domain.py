from packages import *

'''
Plot wrf d01 and draw  box of d02
'''

def plot_wrf_domain(d01_path, d02_path, save = False):
    # open file one
    file_1 = xr.open_dataset(d01_path)
    file_2 = xr.open_dataset(d02_path)
    
    # extract the height variable from domain 1
    height = file_1['HGT'].isel(Time=0)
    
    # extract the lat/lon coordinates for domain 1
    lat = file_1.XLAT.isel(Time=0)
    lon = file_1.XLONG.isel(Time=0)
    
    # Get inner domain boundaries
    file_2_latmin = file_2.XLAT.isel(Time=0).min().values
    file_2_latmax = file_2.XLAT.isel(Time=0).max().values
    
    file_2_lonmin = file_2.XLONG.isel(Time=0).min().values
    file_2_lonmax = file_2.XLONG.isel(Time=0).max().values
    
    # plot height of domain 1 and map out domain 2 using the domain boundaries
    fig, ax = plt.subplots(figsize=(6, 4))
    
    im = ax.imshow(height, extent=(lon.min(), lon.max(), lat.min(), lat.max()), cmap='terrain', origin='lower', alpha=1.0)
    
    # Draw a rectangle around the inner domain boundaries
    rect = patches.Rectangle(
        (file_2_lonmin, file_2_latmin),
        file_2_lonmax - file_2_lonmin,
        file_2_latmax - file_2_latmin,
        linewidth=2,
        edgecolor='black',
        facecolor='none'
    )
    ax.add_patch(rect)
    
    # Add text labels for domains
    ax.text(lon.min(), lat.max(), 'D01', color='white', fontsize=12, fontweight='bold', va='top', ha='left')
    ax.text(file_2_lonmin, file_2_latmax, 'D02', color='black', fontsize=12, fontweight='bold', va='bottom', ha='left')
    
    plt.title('Elevation (m)')
    
    # Modify latitude and longitude labels
    lon_ticks = ax.get_xticks()
    lat_ticks = ax.get_yticks()
    lon_labels = [f'{abs(lon):.0f}°{"W" if lon < 0 else "E"}' for lon in lon_ticks]
    lat_labels = [f'{abs(lat):.0f}°{"S" if lat < 0 else "N"}' for lat in lat_ticks]
    ax.set_xticklabels(lon_labels)
    ax.set_yticklabels(lat_labels)
    
    # Add gridlines
    ax.grid(color='gray', linestyle='--', linewidth=0.2)
    
    # Create a colorbar with the same height as the plot
    divider = make_axes_locatable(ax)
    cax = divider.append_axes('right', size='5%', pad=0.05)
    plt.colorbar(im, cax=cax)
    
    if save:
        plt.savefig('WRF_domain.pdf', dpi=300)    
    
    # Display the plot
    plt.show()

'''
Make spatial plots of snodas vs wrf spatial output
'''
def make_snodas_Wrf_plots(file_list, title, lat, lon, save_title,label, show=True,\
                          subplots=(1,3), colour='terrain', size = (10, 15),save=True):

    vmax = max(np.nanmax(ds.values) for ds in file_list)
    vmin = min(np.nanmin(ds.values) for ds in file_list)

    fig = plt.figure(figsize=size)
        
    grid = ImageGrid(fig, 111,          # as in plt.subplot(111)
                 nrows_ncols=subplots,
                 axes_pad=0.3,
                 share_all=True,
                 cbar_location="bottom",
                 cbar_mode="single",
                 cbar_size="4%",  
                 cbar_pad=0.15,
                )

    for i, ax in enumerate(grid):
        im = ax.imshow(file_list[i],extent=(lon.min(), lon.max(), lat.min(),\
                                            lat.max()),vmax=vmax, vmin = vmin, cmap=colour, origin='lower', alpha=1.0)
        ax.set_title(title[i])
        ax.xaxis.set_major_locator(plt.MultipleLocator(base=1.0))
        ax.yaxis.set_major_locator(plt.MultipleLocator(base=0.4))
   

    # for ax in ax.flat:
    lon_ticks = ax.get_xticks()
    lat_ticks = ax.get_yticks()
    lon_labels = [f'{abs(lon):.2f}°{"W" if lon < 0 else "E"}' for lon in lon_ticks]
    lat_labels = [f'{abs(lat):.2f}°{"S" if lat < 0 else "N"}' for lat in lat_ticks]
    ax.set_xticklabels(lon_labels)
    ax.set_yticklabels(lat_labels)
    
    # Colorbar
    cbar = ax.cax.colorbar(im)
    cbar_X = ax.cax.toggle_label(True)
    cbar.set_label(label)

    if save:
        plt.savefig(save_title+'.pdf',dpi=600)
    
    if show:
        plt.show()
        
'''
Align axis for subplots

'''
def axis_create(axis,data, lat, lon, vmax, vmin, colour, label):
    im = axis.imshow(data, extent=(lon.min(), lon.max(), lat.min(), lat.max()),\
                     vmax=vmax, vmin = vmin, cmap=colour, origin='lower', alpha=1.0)
    axis.xaxis.set_major_locator(plt.MultipleLocator(base=1.0))
    axis.yaxis.set_major_locator(plt.MultipleLocator(base=0.4))
    lon_ticks = axis.get_xticks()
    lat_ticks = axis.get_yticks()
    lon_labels = [f'{abs(lon):.2f}°{"W" if lon < 0 else "E"}' for lon in lon_ticks]
    lat_labels = [f'{abs(lat):.2f}°{"S" if lat < 0 else "N"}' for lat in lat_ticks]
    axis.set_xticklabels(lon_labels)
    axis.set_yticklabels(lat_labels)
    axis.set_title(label)
    return im, axis


'''
compare spatial plots across schemes and snodas

'''
def peak_compare(files, title,lat,lon,save_name,label,colour = 'RdYlBu_r',save = False):

    vmax = max(np.nanmax(ds.values) for ds in files)
    vmin = min(np.nanmin(ds.values) for ds in files)

    fig = plt.figure(figsize=(10,6))

    # Create subplots using plt.subplot2grid
    ax1 = plt.subplot2grid(shape=(2, 6), loc=(0, 0), colspan=2)
    ax2 = plt.subplot2grid((2, 6), (0, 2), colspan=2)
    ax3 = plt.subplot2grid((2, 6), (0, 4), colspan=2)
    ax4 = plt.subplot2grid((2, 6), (1, 1), colspan=2)
    ax5 = plt.subplot2grid((2, 6), (1, 3), colspan=2)

    im1, ax1 = axis_create(ax1,files[0], lat, lon, vmax, vmin, colour, title[0])
    im2, ax2 = axis_create(ax2,files[1], lat, lon, vmax, vmin, colour, title[1])
    im3, ax3 = axis_create(ax3,files[2], lat, lon, vmax, vmin, colour, title[2])
    im4, ax4 = axis_create(ax4,files[3], lat, lon, vmax, vmin, colour, title[3])
    im5, ax5 = axis_create(ax5,files[4], lat, lon, vmax, vmin, colour, title[4])

    # Turn off axis labels for all other subplots
    for ax in [ax2, ax3, ax5]:
        #ax.set_xticks([])
        ax.set_yticks([])
        
    # Adjust layout spacing by modifying figure size and subplot parameters
    fig = plt.gcf()

    # Add color bar horizontally centered and half the length of the axes
    cbar_ax = fig.add_axes([0.27, 0.1, 0.5, 0.02])  # Adjust the position and size as needed
    cbar = fig.colorbar(im5, cax=cbar_ax, orientation='horizontal')
    cbar.set_label(label)

    plt.subplots_adjust(hspace=0.0)  # Adjust the value as needed
    
    if save:
        plt.savefig(save_name + '.pdf',dpi=300)

    plt.show()
