=============
mobitic_utils
=============


.. image:: https://img.shields.io/pypi/v/mobitic_utils.svg
        :target: https://pypi.python.org/pypi/mobitic_utils

.. image:: https://img.shields.io/travis/haixuantao/mobitic_utils.svg
        :target: https://travis-ci.com/haixuantao/mobitic_utils

.. image:: https://readthedocs.org/projects/mobitic-utils/badge/?version=latest
        :target: https://mobitic-utils.readthedocs.io/en/latest/?badge=latest
        :alt: Documentation Status

Utils function necessary for project related to mobitic such as importing data, common transformation function, etc...

Installation
--------

To install the package you just need to git clone and install with pip:

``
git clone https://git.stable.innovation.insee.eu/mkf1or/mobitic_utils.git
pip3 install -e mobitic_utils/ --proxy http://proxy-rie.http.insee.fr:8080``

Features
--------

``get_population(key=KEY_POPULATION, clean=False) -> pd.DataFrame``

    Get latest estimated population given a key of an S3 Object and return it as a pd.Dataframe.
    The table contains estimation for every hours, day, for every x_tile_up, x_tile_up.

    Args:
        key (str): The S3 key
        
        clean (str): A cleaning process to regroup x and y tiles, 
        transform dates in to datetime types, and set the index to x-y

    Returns:
        pd.DataFrame: Estimated population for every hour date

``get_grille(key=KEY_GRILLE_50, clean=False) -> gpd.GeoDataFrame``

    Get latest grille 50 outputed by the quadtree algorithm

    Args:
        key (str): The S3 key of the grille file
        
        clean (str): A cleaning process to regroup x and y tiles, and remove not essential information: 'x_tile_up', 'y_tile_up', 'scale', 'test', 'n_down', 'id'

    Returns:
        gpd.GeoDataFrame: Grille giving geometry information of every x_tile_up, x_tile_up.

``calculate_average_weeks(populations: pd.DataFrame) -> pd.DataFrame``

    Calculate average estimated population per day of week and hour

    Args:
        populations (pd.DataFrame): Clean population table

    Returns:
        pd.DataFrame: Averaged estimated population per day of week and hour 
    
