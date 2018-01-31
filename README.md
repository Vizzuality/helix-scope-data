# Helix-scope data analysis

[![license](https://img.shields.io/github/license/mashape/apistatus.svg)](https://github.com/Vizzuality/sql2gee/blob/develop/LICENSE)


This repo contains both the data processing component of the [HELIX project](https://github.com/Vizzuality/helix-scope) and the [developer notes](https://github.com/Vizzuality/helix-scope-data/blob/master/work/backend_examples.ipynb).


### Introduction

The data science environment and tools are based on Vizzuality's Data Science [Jupyter Notebooks](https://github.com/Vizzuality/data_sci_tutorials)

Docker has two modes in this project: a *develop* mode, intended for developing in a Jupyter notebook environment, and a *process* mode, intended for parallel processing large volumes of Helix netcdf data.

### Develop mode
To launch the Jupyter notebook environment, start docker via:
```bash
./jupyter.sh develop
```
Then open a browser to `0.0.0.0:8888` to connect to the Jupyter server.

Notebooks and data for this repo are stored in `./Work`.

### Process mode
To launch the processing mode, start docker via:
```bash
./jupyter.sh process
```
This will recursively search through all `.nc` files in your `./data` directory. It will create a list of these files, identify the number of processing cores allocated to your docker session, and distribute processing tasks to reduce all of these individual files amongst the cores. (Note for admin1 resolution each file takes around 2 mins to process.)

After the processing is complete, you will have populated the `./work/processed/` folder with csv tables. These can be combined into a final table by running the function `helix_funcs.combine_processed_result()`

Note, if you re-run this program, by default the program will not overwrite existing data. Therefore you can also partially run this program, and resume it later, or simply update the datasets contained in the `./data` folder and run the program again to create a complete archive of processed files in `./processed`.


#### Processed files

The processed files should maintain an identical folder structure to that contained in the `./work/data/` folder. E.g.:

```bash
.
├── CNRS_data
│   ├── cSoil
│   │   ├── orchidee-giss-ecearth.SWL_15.eco.cSoil.csv
│   │   ├── orchidee-giss-ecearth.SWL_2.eco.cSoil.csv
│   │   ├── orchidee-giss-ecearth.SWL_4.eco.cSoil.csv
│   │   ├── orchidee-ipsl-ecearth.SWL_15.eco.cSoil.csv
│   │   ├── orchidee-ipsl-ecearth.SWL_2.eco.cSoil.csv
│   │   ├── orchidee-ipsl-ecearth.SWL_4.eco.cSoil.csv
│   │   ├── orchidee-ipsl-hadgem.SWL_15.eco.cSoil.csv
│   │   ├── orchidee-ipsl-hadgem.SWL_2.eco.cSoil.csv
│   │   └── orchidee-ipsl-hadgem.SWL_4.eco.cSoil.csv
│   └── cVeg
│       ├── orchidee-giss-ecearth.SWL_15.eco.cVeg.csv
│       ├── orchidee-giss-ecearth.SWL_2.eco.cVeg.csv
│       ├── orchidee-giss-ecearth.SWL_4.eco.cVeg.csv
│       ├── orchidee-ipsl-ecearth.SWL_15.eco.cVeg.csv
│       ├── orchidee-ipsl-ecearth.SWL_2.eco.cVeg.csv
│       ├── orchidee-ipsl-ecearth.SWL_4.eco.cVeg.csv
│       ├── orchidee-ipsl-hadgem.SWL_15.eco.cVeg.csv
│       ├── orchidee-ipsl-hadgem.SWL_2.eco.cVeg.csv
│       └── orchidee-ipsl-hadgem.SWL_4.eco.cVeg.csv
└── UEA_data
    └── climate
        └── pr
            └── ECEARTH-R1.SWL_15.cl.pr.Apr.csv
```

The contents of the files should be as follows (e.g. `./work/processed/UEA_data/climate/pr/ECEARTH-R1.SWL_15.cl.pr.Apr.csv`):

```csv
name_0,iso,id_1,name_1,engtype_1,variable,swl_info,count,max,min,mean,std,impact_tag,institution,model_long_name,model_short_name,model_taxonomy,is_multi_model_summary,is_seasonal,season,is_monthly,month
Mexico,MEX,9,Ciudad de México,Federal District,pr,1.5,3,30.899574279785156,18.50608253479004,23.930439631144207,5.176189890440805,cl,,ECEARTH-R1,ECEARTH,ECEARTH-R1,False,False,,True,Apr
Haiti,HTI,3,L'Artibonite,Department,pr,1.5,7,162.45266723632812,35.2036247253418,93.23222242082868,41.52149089980079,cl,,ECEARTH-R1,ECEARTH,ECEARTH-R1,False,False,,True,Apr
```

The separate outputs can be combined by calling a function from `helix_funcs`. It is at this
point that the data values are modified if necessary, leaving the processed files untouched.
E.g. we reduce the number of significant digits at this stage, so that in the master
output file values are to 1-sig. fig. only:

```python
import helix_funcs
helix_funcs.combine_processed_results('./work/processed'
```

### Variables in output

|variable_name| type| Description|
|-------------|-----|------------|
|name_0| STR| Name of country |
|iso| STR| 3 character country iso code |
|id_1 | INT | ID number of Admin1 area |
|name_1| STR | Name of Admin 1 area |
| engtype_1 | STR | Kind of area |
|variable| STR| variable name |
|swl_info| FLOAT | Specific warming level |
|count| INT | Number of pixels intersecting geometry |
|max| FLOAT | Maximum pixel value intersecting geometry |
|min| FLOAT | Minimum pixel value intersecting geometry |
|mean| FLOAT | Average pixel value intersecting geometry|
|std| FLOAT | Standard Sample Deviation of pixels intersecting geometry|
|impact_tag| STR | code to indicate what impacts variable relates to |
|institution| STR | Institute name |
|model_long_name| STR | Model long name |
|model_short_name| STR | Model Short name |
|model_taxonomy| STR | Model taxonomy |
|is_multi_model_summary| BOOL | Flag to indicate if it is a summary of models |
|is_seasonal| BOOL | Flag to indicate if data are seasonal |
|season| STR | Characters to indicate the month grouping e.g. 'SON','DFJ'|
|is_monthly| BOOL | Flag to indicate if the data relate to one month only |
|month| STR | Calander month, e.g. `Apr`|



## Data sources

This project mixes data from a variety of sources. The majority of
the data are netcdf format, however we also have csv files (e.g. UEA for agriculture)
and Excel workbooks (population data). We will need to process all of these data sources
and create a standardised output (one that shares the same structure) prior to generating
complete master tables for the backend.

### Excel

Before working with the excel files, should first be opened and `save as` a `.csv`
file.

Warning:
* These files mix zeros and NA values.
* They have inconsistent column names between files.

### Shapefiles

Except for the admin0 level table, the
majority of this project depends on intersecting
shapes with a raster (from netcdf)
of data. These shapes need to be custom
generated:

#### Procedure for creating gridded shapefiles

Start with polygons of the global land:
    Source [Natural Earth 50m Land polygons](http://www.naturalearthdata.com/downloads/50m-physical-vectors/)

Load the Natural Earth Polygons in QGis.

In [QGis](https://qgis.org/en/site/), create a Vector Grid that overlays the polygons at your
desired resolution. `Vector > Research Tools > Vector Grid`

At this point, the grid should overlay the land polygons.

Finally, run an intersect algorithm: e.g. `Vector > Geometry processing tools > Intersect`
(Depending on the shape size and complexity this may take some time.)

Finally, before uploading to carto you may need to alter the auto-generated `id` values. The field `id` will not parse. We have been
using Geopandas to extract the geometries and add a new index column called `id_vals`.