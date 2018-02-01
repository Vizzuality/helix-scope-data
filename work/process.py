import helix_funcs
from multiprocessing import Process,Pool,cpu_count
import time
import math
import geopandas as gpd
import glob

def chunks(l, n):
    """Yield successive n-sized chunks from a list l"""
    for i in range(0, len(l), n):
        yield l[i:i + n]

# ------------- FOR NOW SET HERE THE SHAPEFILE TO PROCESS AND CHANGE argument--
# e.g. processes= [Process(target=helix_funcs.process_file, args=(f, s, 'grids'),
#                                                   0 above for admin-0 ^

# ADMIN 0 LEVEL (unsimplifed country shapes)
#s = gpd.read_file('./data/gadm28_countries/gadm28_countries.shp')

# SIMPLIFIED SHAPES FOR ADMIN 1 LEVEL
#s = gpd.read_file("./data/gadm28_adm1_simplified/gadm28_adm1_simplified.shp")

# Gridded (10x10 degree) land-intersected shapes
#s = gpd.read_file("./data/sanitized_grid/sanitized_grid.shp")

# Gridded (5x5 degree) land intersected shapes - use 'grids5' argument
#s = gpd.read_file("./data/good_five_grid/good_five_grid.shp")

# RAW GRID - not assigned to any land-intersected shp(use 'rawgrid' argument)
#s = gpd.read_file("./data/raw_grid/raw_grid.shp")
s = "./data/OneDegInterMod/OneDegInterMod.shp"

s = s.to_crs(epsg='4326')
d = helix_funcs.identify_netcdf_and_csv_files()
fs = d.get('nc')  # Simply break this list by the number of avilable processors

if __name__ == "__main__":
    start = time.time()
    processes= [Process(target=helix_funcs.process_file, args=(f, s, 'grids1'),
                        kwargs={'verbose': True,}) for f in fs]
    for process_chunk in chunks(processes, cpu_count()):
        print('loop ', process_chunk, ' of ', cpu_count())
        [p.start() for p in process_chunk]
        [p.join() for p in process_chunk]
    fin_time = time.time()
    print("\nFinished processing in {0:6.2f} min.".format((fin_time-start)/60.))
