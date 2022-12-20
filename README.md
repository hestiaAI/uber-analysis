# Uber Driver Data Analysis

The entry point to this repository is the Jupyter Notebook named `Uber Analysis.ipynb`, which can be used to analyse Uber data coming from SARs or the Uber Driver Portal.

Another notebook containing previous work done by Emmanuel can be found in the history of the repository and is named `Old Analysis.ipynb`.


## Setting-up
A Python environment can be easily setup using conda based on the `environment.yml` file:
1. install conda following [these instructions](https://docs.conda.io/projects/conda/en/latest/user-guide/install/index.html)
2. create an environment using `conda env create --file=environment.yaml`
3. activate the environment using `conda activate hestia-uber-analysis`

Then , you can simply run `jupyter notebook` and open `Uber Analysys.ipynb`.

If you don't use conda, you can create an environment with venv using the correct Python version (the one specified in `environment.yml`), and run `pip install jupyter numpy pandas portion pyexcelerate geopy`. 