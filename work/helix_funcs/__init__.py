from netCDF4 import Dataset
import os
#import re
import fiona
import re
import rasterio
#import cartoframes
#import matplotlib.pyplot as plt # deactivating this for processing due to issue with PyQt4 in docker
from rasterio.mask import mask
from rasterio.plot import show
from rasterstats import zonal_stats
import geopandas as gpd
import pandas as pd
import numpy as np
from iso3166 import countries
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
    stripped = filepath[5:] # Assume files are always in relative path  "data/"
    split_string = stripped.split('/')
    file_name = split_string[-1]
    # obtain var name from filename not folder path:
    seasons = ["MAM", "JJA", "SON", "DJF"]
    possible_var = split_string[-1].split('.')[-2]
    if possible_var.upper() in seasons:
        variable = split_string[-1].split('.')[-3]
    else:
        variable = possible_var
    model_taxonomy = file_name.split(".")[0]
    model_short_name = model_taxonomy.split("-")[0]
    model_short_name
    # Hack to insert instiution metadata for UCL files, as they unfortunatley
    # left that metada field out of the netcdf files...
    ugly_hack = {}
    if split_string[0] == 'UEA_data':
        ugly_hack['institution'] = "Tyndall Centre for Climate Change Research, University of East Anglia"
    return {'model_short_name':model_short_name,
            'model_taxonomy':model_taxonomy,
            'variable':variable,
            **ugly_hack}


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

# note the below function needs to be re-written following the code update
# as we need to extract the admin level shape attributes at a new point now.
def get_shape_attributes(i, shps, shape_id):
    """Get attributes of shapes for gadm28_admin1 data
    index (i) should be passed.
    shape_id should be an id string of the shape (e.g. 'grids',
    'admin_0', 'admin_1')
    """
    d = {}
    if shape_id == 'rawgrid':
        return {'id_val': shps['id'][i]}
    elif shape_id == 'grids1' or shape_id == 'grids5':
        keys = ['id_val']
        hack_d = {'id_val': 'shape_id'}
    elif shape_id == 'admin_0':
        keys = ['iso', 'name_engli']
        hack_d = {'iso':'iso', 'name_engli':'name_0'}
    elif shape_id == 'admin_1':
        keys = ['iso','name_0','id_1','name_1','engtype_1']
        hack_d = {'iso':'iso', 'name_0':'name_0','id_1':'id_1',
                  'name_1':'name_1','engtype_1':'engtype_1'}
    else:
        raise ValueError("Admin level should be set to 0 or 1")
    for table_attribute in keys:
        try:
            d[hack_d[table_attribute]] = shps[table_attribute][i]
        except:
            pass
    return d


def process_file(file, shps, shape_id, verbose=False, overwrite=False,
                 skip_monthly=True, skip_seasonal=True):
    """Given a single NETCDF file, generate a csv table with the same folder/file
    name in ./data/processed/ with all required csv info.
    The admin level with which to process the file should be specified.
    Expect file to be a string e.g.:
    "data/CNRS_data/cSoil/orchidee-giss-ecearth.SWL_15.eco.cSoil.nc"
    Note: admin0 tables should have aggregated stats, while admin1 tables should
    only contain means.
    shape_id can be 'admin_0', 'admin_1', 'grids1', 'grids5'
    """
    valid_shapes = ['admin_0', 'admin_1', 'grids1', 'grids5']
    if skip_monthly:
        suffix_item = file.split('/')[-1].split('.')[-2]
        months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
                  "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
        if suffix_item.title() in months:
            warning = ('is monthly data. Skipping. '
                        "To process set skip_monthly to False.")
            if verbose: print(file, warning)
            return None
    if skip_seasonal:
        season_values = ['SON', 'JJA', 'DJF', 'MAM']
        suffix_item = file.split('/')[-1].split('.')[-2]
        if suffix_item.upper() in season_values:
            warning = ('is seasonal data. Skipping '
                        'To process set skip_seasonal to False')
            if verbose: print(file, warning)
            return None
    if shape_id == 'grids1':
        admin_prefix = 'grids1/'
        if verbose: print('working on ', admin_prefix)
    elif shape_id == 'grids5':
        admin_prefix = 'grids5/'
        if verbose: print('working on ', admin_prefix)
    elif shape_id == 'admin_0':
        admin_prefix = 'admin0/'
        if verbose: print('working on ', admin_prefix)
    elif shape_id == 'admin_1':
        admin_prefix = 'admin1/'
        if verbose: print('working on ',admin_prefix)
    else:
        raise ValueError("shape_id kwarg must be one of", valid_shapes)
    output_filename = "".join(['./processed/',admin_prefix,file[5:-3],'.csv'])
    if os.path.isfile(output_filename) and not overwrite:
        if verbose: print("{0} output exists.".format(output_filename))
        if verbose: print("   Specifcy overwrite=True to replace it.")
        return
    else:
        if verbose: print("Processing '{}'".format(file))
        if shape_id in ['grids1','grids5']:
            keys = ['shape_id', 'variable','swl_info','count', 'max','min',
                    'mean','std','impact_tag','institution','model_long_name',
                    'model_short_name','model_taxonomy',
                    'is_multi_model_summary','is_seasonal','season',
                    'is_monthly','month']
            stats_to_get=['mean', 'max','min','std','count']
        elif shape_id=='admin_0':
            keys =['name_0','iso','variable','swl_info',
                    'count', 'max','min','mean','std','impact_tag','institution',
                    'model_long_name','model_short_name','model_taxonomy',
                    'is_multi_model_summary','is_seasonal','season','is_monthly',
                    'month']
            stats_to_get=['mean', 'max','min','std','count']
        elif shape_id=='admin_1':
            keys =['name_0','iso','id_1','name_1','engtype_1','variable','swl_info',
                    'mean','impact_tag','institution',
                    'model_long_name','model_short_name','model_taxonomy',
                    'is_multi_model_summary','is_seasonal','season','is_monthly',
                    'month']
            stats_to_get = ['mean', 'count']
        else:
            raise ValueError('shape_id should be one of ', valid_shapes)
        tmp_metadata = generate_metadata(file)
        geo_df = gpd.read_file(shps)
        shape_ids = geo_df['id_val'].values
        stats_per_file = []
        zstats = zonal_stats(shps, file,
                     stats=stats_to_get)
        for n, row in enumerate(zstats):
            if row.get('count', 0) > 0:
                tmp_d = {**row, **tmp_metadata, 'shape_id': shape_ids[n]}
                stats_per_file.append([tmp_d.get(key, None) for key in keys])
        df = pd.DataFrame(stats_per_file, columns=keys)
        path_check = "/".join(output_filename.split("/")[0:-1])
        if not os.path.exists(path_check):
            os.makedirs(path_check)
        df.to_csv(output_filename, index=False)
        return


def identify_run(element):
    """Helper function to check for the presence of a regular expression r[1-9]
    to indicate the model run. If its found, it should be returned, if not, it
    should throw a None.
    """
    regex = r"r[0-9]+"
    try:
        match = re.search(regex, element)
        found = match.group()
        return int(found.split('r')[-1])
    except:
        return None

def combine_processed_results(path='./processed/admin0/',
                              table_name="./master_admin0.csv",
                              drop_szn_mnth=False):
    """Combine all the csv files in the path (e.g. all processed files)
    into a single master table. Add in extra info too like 2character iso codes.
    NOTE: at this point we use a round function to leave only 1 sig fig of data.
    That is done during pd.read_csv().round(1)
    """
    output_files = identify_netcdf_and_csv_files(path)
    frames = [pd.read_csv(csv_file).round(2) for csv_file in output_files['csv']]
    master_table = pd.concat(frames)
    taxonomy = master_table['model_taxonomy']
    new_taxa_column = []
    run_column = []
    for taxa in taxonomy:
        split_taxa = taxa.split('-')
        last_element = split_taxa[-1].lower()
        run_info = identify_run(last_element)
        if run_info:
            new_taxa_column.append(''.join(split_taxa[:-1]))
            run_column.append(run_info)
        else:
            new_taxa_column.append(taxa)
            run_column.append(1)
    master_table['model_taxonomy'] = new_taxa_column
    master_table['run'] =  run_column
    if drop_szn_mnth:
        master_table = master_table.drop(['is_seasonal','season',
                                          'is_monthly','month'], axis=1)
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
