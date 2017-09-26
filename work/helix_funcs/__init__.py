from netCDF4 import Dataset
import os
#import re
import fiona
import rasterio
#import cartoframes
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
    """
    file_metadata = get_nc_attributes(filepath)
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
    stripped = filepath[6:] # Assume that the files are always in relative path  "data/"
    split_string = stripped.split('/')
    file_name = split_string[-1]
    variable = split_string[-2]
    model_taxonomy = file_name.split(".")[0]
    model_short_name = model_taxonomy.split("-")[0]
    model_short_name
    return {'model_short_name':model_short_name, 'model_taxonomy':model_taxonomy, 'variable':variable}


def get_nc_attributes(filepath):
    """ Most info is stored in the files’ global attribute description,
    we will access it using netCDF4.ncattrs function.
    Example:
    ncAttributes('data/CNRS_data/cSoil/orchidee-giss-ecearth.SWL_15.eco.cSoil.nc')
    """
    nc_file = Dataset(filepath, 'r')
    d = {}
    nc_attrs = nc_file.ncattrs()
    for nc_attr in nc_attrs:
        d.update({nc_attr: nc_file.getncattr(nc_attr)})
    could_be_true = ['true', 'True', 'TRUE']
    d['is_multi_model_summary'] = d['is_multi_model_summary'] in could_be_true
    d['is_seasonal'] = d['is_seasonal'] in could_be_true
    return d


def get_shape_attributes(i, shps):
    """Get attributes of shapes for gadm28_admin1 data
    index (i) should be passed
    """
    d = {}
    for table_attribute in ['iso','name_0','id_1','name_1','engtype_1']:
        try:
            d[table_attribute] = shps[table_attribute][i]
        except:
            pass
    return d


def process_file(file, shps, verbose=False, overwrite=False):
    """Given a single file, generate a csv table with the same folder/file name
    in ./data/processed/ with all required csv info.
    Expect file to be a string e.g.:
    "data/CNRS_data/cSoil/orchidee-giss-ecearth.SWL_15.eco.cSoil.nc"
    """
    output_filename = "".join(['./processed/',file[5:-3],'.csv'])
    if os.path.isfile(output_filename) and not overwrite:

        if verbose: print("{0} output exists.".format(output_filename))
        if verbose: print("   Specifcy overwrite=True on process_file() function call if you want to replace it.")
        return
    else:
        if verbose: print("Processing '{}'".format(file))
        keys = ['name_0','iso','id_1','name_1','engtype_1','variable','SWL_info',
                'count', 'max','min','mean','std','impact_tag','institution',
                'model_long_name','model_short_name','model_taxonomy',
                'is_multi_model_summary','is_seasonal']
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
            zstats = zonal_stats(shp, tmp, band=1, stats=['mean', 'max','min','std','count'],
                                 all_touched=True, raster_out=False,
                                 affine=properties['transform'],
                                 no_data=np.nan)
            if zstats[0].get('count', 0) > 0:
                shp_atts = get_shape_attributes(i, shps=shps)
                #print("SHP_ATTS:",shp_atts)
                tmp_d = {**zstats[0], **shp_atts, **tmp_metadata}
                stats_per_file.append([tmp_d.get(key, None) for key in keys])
        df = pd.DataFrame(stats_per_file, columns=keys)
        path_check = "/".join(output_filename.split("/")[0:-1])
        if not os.path.exists(path_check):
            os.makedirs(path_check)
        df.to_csv(output_filename, index=False)
        return


def combine_processed_results(path='./processed', table_name="./master_admin1.csv"):
    """Combine all the csv files in the path (e.g. all processed files)
    into a single master table
    """
    output_files = identify_netcdf_and_csv_files(path)
    frames = [pd.read_csv(csv_file) for csv_file in output_files['csv']]
    master_table = pd.concat(frames)
    master_table.to_csv(table_name, index=False)
    print("Generated {0}: {1:,g} rows of data from {2:,g} sources.".format(table_name,
                                                            len(master_table),
                                                            len(output_files['csv'])
                                                            ))
    return
