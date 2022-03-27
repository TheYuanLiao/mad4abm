import sys
import subprocess
import os
import pandas as pd
import geopandas as gpd


def get_repo_root():
    """Get the root directory of the repo."""
    dir_in_repo = os.path.dirname(os.path.abspath('__file__'))
    return subprocess.check_output('git rev-parse --show-toplevel'.split(),
                                   cwd=dir_in_repo,
                                   universal_newlines=True).rstrip()


ROOT_dir = get_repo_root()
sys.path.append(ROOT_dir)
sys.path.insert(0, ROOT_dir + '/lib')

from lib import preprocess as preprocess


if __name__ == '__main__':
    user = preprocess.keys_manager['database']['user']
    password = preprocess.keys_manager['database']['password']
    port = preprocess.keys_manager['database']['port']
    db_name = preprocess.keys_manager['database']['name']
    flag = 2
    # 1. Load data from raw files stored in dbs/
    # 1.1 Load raw and processed data
    if flag == 1:
        file_list = ['vgr_stops_2019.csv', 'vgr_point_types_2019.csv']
        for file, tb_name in zip(file_list, ('vgr_stops_2019', 'vgr_point_types_2019')):
            print("Loading", file)
            preprocess.raw2chunk2db(file, user, password, port, db_name,
                                    table_name=tb_name, schema_name='public')
            print(file, "done.")
        # 1.2 Load inferred home data
        print("Loading homes...")
        df_home = pd.read_csv(os.path.join(ROOT_dir, "dbs/original_input_data/vgr_homes_2019.csv"))
        preprocess.dump2db_df(df_home, user, password, port, db_name,
                              table_name='vgr_homes_2019', schema_name='public')
        print("...done.")
    if flag == 2:
        # 2. Inject zones
        print("Loading zones...")
        gdf_zones = gpd.GeoDataFrame.from_file(os.path.join(ROOT_dir, 'dbs', 'DeSO', 'DeSO_2018_v2.shp'))
        preprocess.dump2db_gdf(gdf_zones, 'POLYGON', 3006,
                               user, password, port, db_name, table_name='zones', schema_name='public')
        print("...done.")
        # 3. DeSO socioeconomic factors
        print("Loading socioeco_factors...")
        df = pd.read_csv(os.path.join(ROOT_dir, "dbs/DeSO/socioeco_factors.csv"))
        preprocess.dump2db_df(df, user, password, port, db_name, table_name='socioeco_factors', schema_name='public')
        print("...done.")
