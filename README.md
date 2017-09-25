# Helix-scope data analysis

This repo is the data processing component of Vizzuality’s [HELIX project](https://github.com/Vizzuality/helix-scope)

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

[![license](https://img.shields.io/github/license/mashape/apistatus.svg)](https://github.com/Vizzuality/sql2gee/blob/develop/LICENSE)
