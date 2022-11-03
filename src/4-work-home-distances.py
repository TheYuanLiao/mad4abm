import sys
import subprocess
import os
import pandas as pd
from datetime import datetime
import sqlalchemy
import numpy as np
import time
from tqdm import tqdm


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
    print(f'Loading data:')
    start = time.time()
    workhome_distance = ap.WorkHomeTemporalDistance()
    workhome_distance.load_process_data()
    workhome_distance.add_weight2records()
    workhome_distance.load_templates()
    end = time.time()
    time_elapsed = (end - start) // 60  # in minutes
    print(f"Data processed in {time_elapsed} minutes.")

    top_n = 3
    print(f'Calculating temporal distances of top {top_n} clusters:')
    start = time.time()
    engine = sqlalchemy.create_engine(
        f'postgresql://{workhome_distance.user}:{workhome_distance.password}'
        f'@localhost:{workhome_distance.port}/{workhome_distance.db_name}')
    df_top = pd.read_sql_query(sql="""SELECT * FROM description.clusters_top%s_wt;"""%top_n,
                               con=engine)
    print(f'Keep those devices with more than one cluster.')
    df_top_size = df_top['uid'].value_counts().rename_axis('uid').reset_index(name='cluster_num')
    df_top = df_top.loc[df_top.uid.isin(df_top_size.loc[df_top_size.cluster_num > 1, 'uid']), :]
    workhome_distance.activities_temporal_distance(df_top=df_top)
    end = time.time()
    time_elapsed = (end - start) // 60  # in minutes
    print(f"Cluster temporal distance to home and work computed in {time_elapsed} minutes.")
