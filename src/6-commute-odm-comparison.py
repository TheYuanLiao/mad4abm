import sys
import subprocess
import os
import time
import pandas as pd
import sqlalchemy


def get_repo_root():
    """Get the root directory of the repo."""
    dir_in_repo = os.path.dirname(os.path.abspath('__file__'))
    return subprocess.check_output('git rev-parse --show-toplevel'.split(),
                                   cwd=dir_in_repo,
                                   universal_newlines=True).rstrip()


ROOT_dir = get_repo_root()
sys.path.append(ROOT_dir)
sys.path.insert(0, ROOT_dir + '/lib')

from lib import activity_patterns as ap


if __name__ == '__main__':
    print(f'Loading data and get distance thresholds:')
    start = time.time()
    commute_odms = ap.ODMComparison()
    dist_thre = commute_odms.distance_thresholds()
    end = time.time()
    time_elapsed = (end - start) // 60  # in minutes
    print(f"Data processed in {time_elapsed} minutes.")

    print(f'Calculating MAD OD pairs:')
    start = time.time()
    df_dict_deso = commute_odms.mad_odm(dist_thre=dist_thre, level='DeSO')
    df_dict_muni = commute_odms.mad_odm(dist_thre=dist_thre, level='Municipality')
    end = time.time()
    time_elapsed = (end - start) // 60  # in minutes
    print(f"MAD OD pairs calculated in {time_elapsed} minutes.")

    print(f'Aggregate ODMs:')
    start = time.time()
    df_deso = commute_odms.odm_aggregation(df_dict=df_dict_deso, level='DeSO')
    df_muni = commute_odms.odm_aggregation(df_dict=df_dict_muni, level='Municipality')
    df_odms = pd.concat([df_deso, df_muni])
    # Save data
    engine = sqlalchemy.create_engine(
        f'postgresql://{commute_odms.user}:{commute_odms.password}'
        f'@localhost:{commute_odms.port}/{commute_odms.db_name}')
    df_odms.to_sql('commute', engine, schema='odm', index=False, if_exists='replace',
                   method='multi', chunksize=5000)
    end = time.time()
    time_elapsed = (end - start) // 60  # in minutes
    print(f"ODMs aggregated and saved in {time_elapsed} minutes.")

    print(f'Compare ODMs:')
    start = time.time()
    df_ssi = commute_odms.odm_comparison(df_odms=df_odms, gt_field='sv_commute', mad_field='mad_commute')
    df_ssi.to_csv(os.path.join(ROOT_dir, 'results/commute_odm_comparison.csv'), index=False)
    end = time.time()
    time_elapsed = (end - start) // 60  # in minutes
    print(f"Comparison finished in {time_elapsed} minutes.")