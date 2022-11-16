# Replication codes for "Temporally Consistent Present Population from Mobile Phone Signaling Data for Official Statistics"
--------
by Milena Suarez Castillo, François Sémécurbe, Cezary Ziemlicki, Haixuan Xavier Tao and Tom Seimandi

The pipeline is organized in several modules as follows 

![](docs/schemaPipeline.png)

Figure: _Overview of the method implementation._ Red boxes represent source datasets, dark
blue boxes represent produced datasets and the key variables. Arrows represent transforms, which pertain
to a given module referenced in light blue boxes.



* Pseudo-code (folder `pseudo-code`) implements the modules "Device presence panel", "Residency characterisation (home celle algorithm)", "weighting", "presence estimation (aggregation)". 

* A python package (folder `mobitic_utils`) implements the modules "residency-based weighting" (`weighting.py`), "spatial mapping" (`spatial_mapping.py`), and the different outputs and figures of the paper. Output statistics are uploaded into a postgis database, and this code implements utils to do so.

* Figures and Tables from the paper have been obtained with the notebooks in the `notebook` folder, which call the python package.




Installation
--------

To install the `mobitic_utils` package you just need to git clone and install with pip:

``git clone https://git.lab.sspcloud.fr/telmob/jos-data.git  
pip3 install josdata/ --user ``



Populate PostGreDB with Dynamic Population Data
--------

``mobitic_utils init-db``
or
``python3 mobitic_utils/__main__.py init-db``

See `mobitic_utils/constants.py` for parameters


Comparison metrics (Table 3 of Journal of Official Statistics paper)
--------

 `mobitic_utils/metrics.py`  

 `notebook/table3PopulationDensityComparison.ipynb`

Maps and Figures
--------

`notebook/*`

The four animated figures displayed below are in the `gifs` folder and give the variation of present population density (persons per square kilometer), hourly within day and daily within week, at the national level and in Paris.

<u>France, hourly and daily variations :</u>

<p float="left">
  <img src="gifs/france_day_densities.gif" alt="fr_day" width="400"/>
  <img src="gifs/france_week_densities.gif" alt="fr_week" width="400"/>
</p>

<u>Paris, hourly and daily variations :</u>

<p float="left">
  <img src="gifs/paris_day_densities.gif" alt="paris_day" width="400"/>
  <img src="gifs/paris_week_densities.gif" alt="paris_day" width="400"/>
</p>
