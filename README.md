# Uber Driver Data Analysis

The entry point to this repository is the Jupyter Notebook named `Pipeline.ipynb`, which can be used to analyse Uber data coming from SARs or the Uber Driver Portal.

There is also a notebook called `Distance Analysis.ipynb` that is the result of a quick exploration of the ride distances.

Another notebook containing previous work can be found in the history of the repository and is named `Old Analysis.ipynb`.


## Setting-up

It's recommended to use a virtual environment. You can set it up either with conda or with venv.

### conda
A Python environment can be easily setup using conda based on the `environment.yml` file:
1. install conda following [these instructions](https://docs.conda.io/projects/conda/en/latest/user-guide/install/index.html)
2. create an environment using `conda env create --file=environment.yaml`
3. activate the environment using `conda activate hestia-uber-analysis`

Then, you can simply run `jupyter notebook` and open `Pipeline.ipynb`.

### venv
See this [documentation](https://www.geeksforgeeks.org/using-jupyter-notebook-in-virtual-environment/).

If you don't use conda, you can create an environment with venv using the correct Python version (the one specified in `environment.yml`), 

``` sh
python3 -m venv ./myenv
source ./myenv/bin/activate
# on windows 
./myenv/Scripts/activate.bat
# in git bash for windows
source ./myenv/Scripts/activate
```

and run 
``` sh
pip install jupyter numpy pandas portion pyexcelerate geopy swifter openpyxl
```

for the Distance analysis notebook

``` sh
pip install requests seaborn statsmodels ipynb
pip install -U scikit-learn
```

Install jupyter kernel for the virtual environment with
``` sh
ipython kernel install --user --name=myenv
```

Then, you can run `jupyter notebook` and open `Pipeline.ipynb`.

Don't forget to select the kernel from the python notebook by clicking **Kernel > Change Kernel > myenv**

## Osrm
To run the notebook "Distance Analysis.ipynb" you need the osrm-backend .

Here's how I got it to run on windows, in powershell ( following the instructions at https://github.com/Project-OSRM/osrm-backend),

with a map for switzerland http://download.geofabrik.de/europe/switzerland-latest.osm.pbf

``` sh
 docker run -t -v "c:/Users/andre/workspace/hestia/uber-data/data:/data" ghcr.io/project-osrm/osrm-backend osrm-extract -p /opt/car.lua /data/switzerland-latest.osm.pbf
 
 docker run -t -v "c:/Users/andre/workspace/hestia/uber-data/data:/data" ghcr.io/project-osrm/osrm-backend osrm-partition /data/switzerland-latest.osrm
 
 docker run -t -v "c:/Users/andre/workspace/hestia/uber-data/data:/data" ghcr.io/project-osrm/osrm-backend osrm-customize /data/switzerland-latest.osrm
 
docker run -t -i -p 5000:5000 -v "c:/Users/andre/workspace/hestia/uber-data/data:/data" ghcr.io/project-osrm/osrm-backend osrm-routed --algorithm mld /data/switzerland-latest.osrm
```


## Runnings scripts and tests

``` sh
python3 -m src.tripmatch [path to sar zip]
python3 -m src.test_tripmatch
```
