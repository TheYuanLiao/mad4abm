import sys
import subprocess
import os
import time


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
    commute_zones = ap.CommuteODM()
    commute_zones.load_data()
    end = time.time()
    time_elapsed = (end - start) // 60  # in minutes
    print(f"Data processed in {time_elapsed} minutes.")

    print(f'Calculate centroids:')
    start = time.time()
    commute_zones.cluster_centroids()
    end = time.time()
    time_elapsed = (end - start) // 60  # in minutes
    print(f"Cluster centroids computed in {time_elapsed} minutes.")

    print(f'Finding zones and save:')
    start = time.time()
    commute_zones.find_zones()
    end = time.time()
    time_elapsed = (end - start) // 60  # in minutes
    print(f"Zones computed in {time_elapsed} minutes.")
