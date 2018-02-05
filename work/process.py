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

s = "./data/OneDegInterMod/OneDegInterMod.shp"
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
