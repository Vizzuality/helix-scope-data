import helix_funcs
from multiprocessing import Process,Pool,cpu_count
import time
import math
import glob

s = gpd.read_file('./data/gadm28_adm1/gadm28_adm1.shp')
s = s.to_crs(epsg='4326')
d = helix_funcs.identify_netcdf_and_csv_files()
fs = d.get('nc')  # Simply break this list by the number of avilable processors
                  # and run something like the following for each processor

# for f in fs:
#   helix_funcs.process_file(file=f, shps=s)


if __name__ == "__main__":
    start = time.time()
    cpus = cpu_count() -1
    # total_files = files
    # for nfile in nc_list:
    #     print('Running on:', nfile[50:])
    #     processes= [Process(target=MT_Means_Over_Lake, args=(nfile,lake_data,\
    #                 i,out_path,), kwargs={'plots':False,'rprt_tme':False,})\
    #                                 for i in range(10)] # Run x lakes
    #
    #     cpus = cpu_count() -1
    #     p_groups= int(math.ceil(float(len(processes))/float(cpus)))
    #     srng = 0
    #     frng = cpus
    #     for n in xrange(p_groups):
    #         [p.start() for p in processes[srng:frng]]
    #         [p.join() for p in processes[srng:frng]]
    #         print('loop',n,' of ', p_groups,'  group:',srng,':',frng)
    #         srng+= cpus
    #         frng+= cpus
    #         Update_Progress(float(n)/(float(p_groups)-1.))
    fin_time = time.time()
    print("\nFinished processing in {0:6.2f} min.".format((fin_time-start)/60.))
