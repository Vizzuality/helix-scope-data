from netCDF4 import Dataset
import os
#import re
import fiona
import rasterio
#import cartoframes
#import matplotlib.pyplot as plt # deactivating this for processing due to issue with PyQt4 in docker
from rasterio.mask import mask
from rasterio.plot import show
from rasterstats import zonal_stats
import geopandas as gpd
import pandas as pd
import numpy as np
import datetime
import warnings
warnings.filterwarnings('ignore')


def identify_netcdf_and_csv_files(path='data'):
    """Crawl through a specified folder and return a dict of the netcdf d['nc']
    and csv d['csv'] files contained within.
    Returns something like
    {'nc':'data/CNRS_data/cSoil/orchidee-giss-ecearth.SWL_15.eco.cSoil.nc'}
    """
    netcdf_files = []
    csv_files = []
    for root, dirs, files in os.walk(path):
        if isinstance([], type(files)):
            for f in files:
                if f.split('.')[-1] in ['nc']:
                    netcdf_files.append(''.join([root,'/',f]))
                elif  f.split('.')[-1] in ['csv']:
                    csv_files.append(''.join([root,'/',f]))
    return {'nc':netcdf_files,'csv':csv_files}


def generate_metadata(filepath):
    """Pass a path and file as a sigle string. Expected in the form of:
        data/CNRS_data/cSoil/orchidee-giss-ecearth.SWL_15.eco.cSoil.nc
    Returns a dictionary of prepared metadata based on filename, path,
    and netcdf attributes.
    """
    tmp_metadata = get_nc_attributes(filepath)
    file_metadata = prep_nc_attributes(tmp_metadata)
    filename_properties = extract_medata_from_filename(filepath)
    return {**file_metadata, **filename_properties}


def extract_medata_from_filename(filepath):
    """Given a filepath that starts in a relative path of 'data/'
     extract model metadta from the filename.
     e.g. data/CNRS_data/cSoil/orchidee-giss-ecearth.SWL_15.eco.cSoil.nc
     yields:
             {'model_short_name': 'orchidee',
              'model_taxonomy': 'orchidee-giss-ecearth',
              'variable': 'cSoil'}
    """
    stripped = filepath[6:] # Assume files are always in relative path  "data/"
    split_string = stripped.split('/')
    file_name = split_string[-1]
    variable = split_string[-2]
    model_taxonomy = file_name.split(".")[0]
    model_short_name = model_taxonomy.split("-")[0]
    model_short_name
    return {'model_short_name':model_short_name,
            'model_taxonomy':model_taxonomy,
            'variable':variable}


def prep_nc_attributes(d):
    """
    This should return an extracted and cleaned version of the raw netcdf atts.
    It is this dictionary that should be used to create the summary table.
    We can't assume that the raw netcdf attributes have been standardised
    (i.e. correct case). We also cant assume that the flag attributes are
    present. (They should be in the resulting table.)
    """
    sanitized = {}
    for key in d:
        if key.lower() == 'is_multi_model_summary':
            # This is the pattern to convert to a boolean
            sanitized['is_multi_model_summary'] = d.get(key).capitalize() == 'True'
        if key.lower() == 'is_seasonal':
            sanitized['is_seasonal'] = d.get(key).capitalize() == 'True'
        if key.lower() == 'is_monthly':
            sanitized['is_monthly'] = d.get(key).capitalize() == 'True'
        if key.lower() == 'impact_tag':
            # If we need to have a value in the table:
            sanitized['impact_tag'] = d.get(key).strip()
        if key.lower() == 'season':
            sanitized['season'] = d.get(key).strip()
        if key.lower() == 'month':
            sanitized['month'] = d.get(key).strip()
        if key.lower() == 'swl_info':
            sanitized['swl_info'] = float(d.get(key, None))
        if key.lower() == 'institution':
            sanitized['institution'] = d.get(key).strip()
        if key.lower() == 'model_long_name':
            sanitized['model_long_name'] = d.get(key).strip()
        # For keys of flags have not been included, ensure a False value appears
        if not sanitized.get('is_seasonal', None):
            sanitized['is_seasonal'] = False
        if not sanitized.get('is_monthly', None):
            sanitized['is_monthly'] = False
        if not sanitized.get('is_multi_model_summary', None):
            sanitized['is_multi_model_summary'] = False
    return sanitized


def get_nc_attributes(filepath):
    """ Most info is stored in the files’ global attribute description,
    we will access it using netCDF4.ncattrs function.
    Example:
    ncAttributes('data/CNRS_data/cSoil/orchidee-giss-ecearth.SWL_15.eco.cSoil.nc')
    This should be returned un-mutated from this function
    """
    nc_file = Dataset(filepath, 'r')
    d = {}
    nc_attrs = nc_file.ncattrs()
    for nc_attr in nc_attrs:
        d.update({nc_attr: nc_file.getncattr(nc_attr)})
    return d


def get_shape_attributes(i, shps, admin_level):
    """Get attributes of shapes for gadm28_admin1 data
    index (i) should be passed
    """
    d = {}
    if admin_level == 0:
        keys = ['iso','name_0']
    elif admin_level == 1:
        keys = ['iso','name_0','id_1','name_1','engtype_1']
    else:
        raise ValueError("Admin level should be set to 0 or 1")
    for table_attribute in keys:
        try:
            d[table_attribute] = shps[table_attribute][i]
        except:
            pass
    return d


def process_file(file, shps, admin_level, verbose=False, overwrite=False):
    """Given a single file, generate a csv table with the same folder/file name
    in ./data/processed/ with all required csv info.
    The admin level with which to process the file should be specified.
    Expect file to be a string e.g.:
    "data/CNRS_data/cSoil/orchidee-giss-ecearth.SWL_15.eco.cSoil.nc"
    Note: admin0 tables should have aggregated stats, while admin1 tables should
    only contain means.
    """
    if admin_level == 0:
        admin_prefix = 'admin0/'
        if verbose: print('working on ', admin_prefix)
    elif admin_level == 1:
        admin_prefix = 'admin1/'
        if verbose: print('working on ',admin_prefix)
    else:
        raise ValueError("admin_level kwarg must be either 0 or 1")
    output_filename = "".join(['./processed/',admin_prefix,file[5:-3],'.csv'])
    if os.path.isfile(output_filename) and not overwrite:

        if verbose: print("{0} output exists.".format(output_filename))
        if verbose: print("   Specifcy overwrite=True to replace it.")
        return
    else:
        if verbose: print("Processing '{}'".format(file))
        if admin_level==0:
            keys =['name_0','iso','variable','swl_info',
                    'count', 'max','min','mean','std','impact_tag','institution',
                    'model_long_name','model_short_name','model_taxonomy',
                    'is_multi_model_summary','is_seasonal','season','is_monthly',
                    'month']
            stats_to_get=['mean', 'max','min','std','count']
        elif admin_level==1:
            keys =['name_0','iso','id_1','name_1','engtype_1','variable','swl_info',
                    'mean','impact_tag','institution',
                    'model_long_name','model_short_name','model_taxonomy',
                    'is_multi_model_summary','is_seasonal','season','is_monthly',
                    'month']
            stats_to_get = ['mean', 'count']
        else:
            raise ValueError('Admin_level should be 0 or 1')
        tmp_metadata = generate_metadata(file)
        with rasterio.open(file) as nc_file:
            rast=nc_file.read()
            properties = nc_file.profile
        tmp = rast[0,:,:]
        mask = tmp == properties.get('nodata')
        tmp[mask] = np.nan
        stats_per_file = []
        for i in shps.index:
            shp = shps.iloc[i].geometry
            zstats = zonal_stats(shp, tmp, band=1,
                                 stats=stats_to_get,
                                 all_touched=True, raster_out=False,
                                 affine=properties['transform'],
                                 no_data=np.nan)
            if zstats[0].get('count', 0) > 0:
                shp_atts = get_shape_attributes(i, shps=shps, admin_level=admin_level)
                tmp_d = {**zstats[0], **shp_atts, **tmp_metadata}
                stats_per_file.append([tmp_d.get(key, None) for key in keys])
        df = pd.DataFrame(stats_per_file, columns=keys)
        path_check = "/".join(output_filename.split("/")[0:-1])
        if not os.path.exists(path_check):
            os.makedirs(path_check)
        df.to_csv(output_filename, index=False)
        return


def combine_processed_results(path='./processed/admin1/',
                              table_name="./master_admin1.csv"):
    """Combine all the csv files in the path (e.g. all processed files)
    into a single master table.
    NOTE: at this point we use a round function to leave only 1 sig fig of data.
    That is done during pd.read_csv().round(1)
    """
    output_files = identify_netcdf_and_csv_files(path)
    frames = [pd.read_csv(csv_file).round(1) for csv_file in output_files['csv']]
    master_table = pd.concat(frames)
    master_table.to_csv(table_name, index=False)
    print("Made {0}: {1:,g} rows of data. {2:,g} sources.".format(table_name,
                                                        len(master_table),
                                                        len(output_files['csv'])
                                                                 ))
    return


def map_file_by_iso(f, s, iso="ESP", var='mean'):
    """Read a processed CSV (expecting admin1 level) and produce a
    sample choropleth plot to check how the data processing went.
    f is filepath e.g. 'processed/CNRS_data/cSoil/orchidee-ipsl-hadgem.SWL_2.eco.cSoil.csv'
    s are loaded geopandas dataframe e.g: s = gpd.read_file('./data/gadm28_adm1/gadm28_adm1.shp')
    The shapes should, in this case, not be simplified shapes.
    """
    f_split = f.split('/')
    f_split
    title = " ".join([iso,":",f_split[-1].split('.csv')[0]])
    df = pd.read_csv(f)
    country_subset = df['iso'] == iso
    keys = ['iso', 'id_1', var]
    country_subset = df['iso'] == iso
    extrated_data = []
    for row in df[country_subset].index:
        extrated_data.append([df[k][row] for k in keys])
    tmp_df = pd.DataFrame(extrated_data, columns = keys)
    s_smaller = s[s['iso'] == iso]
    geoms = []
    for row in tmp_df.index:
        s_smaller_mask = tmp_df['id_1'][row] == s_smaller['id_1']
        geoms.append(s_smaller[s_smaller_mask].geometry.values[0])#.simplify(0.01))
    map_data = gpd.GeoDataFrame(tmp_df, geometry=geoms)
    # Plotting section
    fig, ax = plt.subplots()
    ax.set_aspect('equal')
    s_smaller.plot(ax=ax, color='red', linewidth=0.5) # Basemap of all country shapes (red polygons)
    map_data.plot(ax=ax, column='mean',cmap='Pastel1', alpha=1.0, linewidth=0.5)  # All shapes for where there exist data (colored polygons)
    plt.title(title)
    outfname = "".join(["./",iso,".",f_split[-1].split('.csv')[0],'.png'])
    plt.savefig(outfname, dpi=300)
    print("Written {0}".format(outfname))
    return tmp_df
