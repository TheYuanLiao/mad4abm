import pandas as pd
import os
import sys
import subprocess
import numpy as np
import sqlalchemy
from tqdm import tqdm
from datetime import datetime
from shapely.geometry import MultiPoint
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


class ActivityPatterns:
    def __init__(self):
        """
        :param month: 06, 07, 08, 12
        :type month: str
        :return: None
        :rtype: None
        """
        self.user = preprocess.keys_manager['database']['user']
        self.password = preprocess.keys_manager['database']['password']
        self.port = preprocess.keys_manager['database']['port']
        self.db_name = preprocess.keys_manager['database']['name']
        self.data = None
        self.clusters = None

    def load_process_data(self):
        engine = sqlalchemy.create_engine(
            f'postgresql://{self.user}:{self.password}@localhost:{self.port}/{self.db_name}')
        self.data = pd.read_sql_query(
            sql="""SELECT uid, location_method, "TimeLocal", "leaving_TimeLocal", date, dur, month, cluster FROM 
            stops_subset;""",
            con=engine)
        # Add start time hour and duration in minute
        self.data.loc[:, 'h_s'] = self.data.loc[:, 'TimeLocal'].apply(lambda x: x.hour * 60 + x.minute)
        self.data.loc[:, 'dur'] = self.data.loc[:, 'dur'] / 60
        # Mark holiday season boundaries
        summer_start = datetime.strptime("2019-06-23 00:00:00", "%Y-%m-%d %H:%M:%S")
        summer_end = datetime.strptime("2019-08-11 00:00:00", "%Y-%m-%d %H:%M:%S")
        christmas_start = datetime.strptime("2019-12-22 00:00:00", "%Y-%m-%d %H:%M:%S")

        def holiday(x, s1, s2, s3):
            if (s1 < x < s2) | (x > s3):
                return 1
            else:
                return 0

        # Add holiday season label
        tqdm.pandas()
        self.data.loc[:, 'holiday'] = self.data.loc[:, 'TimeLocal'].progress_apply(
            lambda x: holiday(x, summer_start, summer_end, christmas_start))

    def aggregate_activity_temporal(self):
        # All records of stays
        all = list(self.data.loc[:, ['h_s', 'dur']].to_records(index=False))
        df_all = preprocess.cluster_tempo(pur='all', temps=all)
        # Holiday
        holidays = list(self.data.loc[self.data.holiday == 1, ['h_s', 'dur']].to_records(index=False))
        df_holidays = preprocess.cluster_tempo(pur='holiday', temps=holidays)
        # Non-holiday
        non_holidays = list(self.data.loc[self.data.holiday == 0, ['h_s', 'dur']].to_records(index=False))
        df_nholidays = preprocess.cluster_tempo(pur='non_holiday', temps=non_holidays)
        return pd.concat([df_all, df_holidays, df_nholidays])

    def add_weight2records(self):
        def record_weights(data):
            # Get weights
            recs = list(data[['h_s', 'dur']].to_records(index=False))
            df_tp = preprocess.cluster_tempo(temps=recs, prt=False)
            df_tp.loc[:, 'wt'] = df_tp.loc[:, 'freq'].apply(lambda x: 1 / x if x != 0 else 0)
            wt = df_tp.loc[:, 'wt'].values.reshape((48, 1))

            # Assign weights to each location
            def row_weight_assign(row):
                start_ = int(np.floor(row['h_s'] / 30))
                end_ = int(np.floor((row['h_s'] + int(row['dur'])) / 30))
                return np.sum(wt[start_:end_ + 1, 0])

            data.loc[:, 'wt'] = data.apply(lambda row: row_weight_assign(row), axis=1)
            return data
        tqdm.pandas()
        self.data = self.data.groupby('uid').progress_apply(record_weights).reset_index(drop=True)

    def cluster_stats(self, top_n=3):
        def cluster_attrs(data):
            freq = len(data)
            freq_wt = sum(data.wt)
            dur = data.dur.sum()
            return pd.Series(dict(freq=freq, freq_wt=freq_wt, dur=dur))

        # Computing the statistics of clusters
        print('Computing statistics of clusters...')
        tqdm.pandas()
        self.clusters = self.data.groupby(['uid', 'cluster', 'holiday']).progress_apply(cluster_attrs).reset_index()
        self.clusters = self.clusters.sort_values(by=['uid', 'holiday', 'freq_wt'], ascending=[True, True, False])
        print('Saving clusters statistics...')
        engine = sqlalchemy.create_engine(
            f'postgresql://{self.user}:{self.password}@localhost:{self.port}/{self.db_name}')
        self.clusters.to_sql('clusters', engine, schema='description', index=False, if_exists='replace',
                             method='multi', chunksize=5000)

        print('Selecting top 3 clusters individually...')
        tqdm.pandas()
        # Weighted top clusters
        df_top = self.clusters.loc[self.clusters.holiday == 0, :].groupby('uid').head(top_n).reset_index(drop=True)
        engine = sqlalchemy.create_engine(
            f'postgresql://{self.user}:{self.password}@localhost:{self.port}/{self.db_name}')
        df_top.to_sql(f'clusters_top{top_n}_wt', engine, schema='description', index=False, if_exists='replace',
                      method='multi', chunksize=5000)

        # Unweighted top clusters
        self.clusters = self.clusters.sort_values(by=['uid', 'holiday', 'freq'], ascending=[True, True, False])
        df_top = self.clusters.loc[self.clusters.holiday == 0, :].groupby('uid').head(top_n).reset_index(drop=True)
        engine = sqlalchemy.create_engine(
            f'postgresql://{self.user}:{self.password}@localhost:{self.port}/{self.db_name}')
        df_top.to_sql(f'clusters_top{top_n}', engine, schema='description', index=False, if_exists='replace',
                      method='multi', chunksize=5000)

    def activities_temporal(self, df_top=None):
        # Calculate temporal patterns of each cluster
        def cluster_tempo_agg(data):
            recs = list(data[['h_s', 'dur', 'wt']].to_records(index=False))
            df_tp = preprocess.cluster_tempo_weighted(temps=recs, prt=False)
            df_tp.loc[:, 'uid'] = data.uid.values[0]
            df_tp.loc[:, 'cluster'] = data.cluster.values[0]
            return df_tp

        print('Get records only for the input clusters of individuals...')
        self.data = pd.merge(self.data, df_top[['uid', 'cluster']], on=['uid', 'cluster'], how='inner')
        print('Extracting activity temporal profiles for top 3...')
        tqdm.pandas()
        df_tempo = self.data.groupby(['uid', 'cluster']).progress_apply(cluster_tempo_agg).reset_index(drop=True)
        df_tempo_list = preprocess.df2batches(df_tempo, chunk_size=10000000)
        del df_tempo
        for df in tqdm(df_tempo_list, desc='Saving temporal profiles'):
            engine = sqlalchemy.create_engine(
                f'postgresql://{self.user}:{self.password}@localhost:{self.port}/{self.db_name}')
            df.to_sql('tempo_top3', engine, schema='description', index=False, if_exists='append',
                         method='multi', chunksize=5000)


class WorkHomeTemporalDistance(ActivityPatterns):
    def __init__(self):
        super().__init__()
        self.template = None

    def load_templates(self):
        df_survey = pd.read_csv(os.path.join(ROOT_dir, 'results', 'activity_patterns_survey.csv'))
        self.template = dict(home=df_survey.loc[df_survey.activity == 'Home', 'freq'].values,
                             work=df_survey.loc[df_survey.activity == 'Work', 'freq'].values)

    def activities_temporal_distance(self, df_top=None):
        # Calculate temporal patterns of each cluster
        def cluster_tempo_distance(data):
            recs = list(data[['h_s', 'dur', 'wt']].to_records(index=False))
            df_tp = preprocess.cluster_tempo_weighted(temps=recs, prt=False)
            # Calculate distance to home and work templates
            dist2home = np.linalg.norm(df_tp.freq.values - self.template['home'])
            dist2work = np.linalg.norm(df_tp.freq.values - self.template['work'])
            dist2home_wt = np.linalg.norm(df_tp.freq_wt.values - self.template['home'])
            dist2work_wt = np.linalg.norm(df_tp.freq_wt.values - self.template['work'])
            return pd.Series(dict(dist2home=dist2home, dist2work=dist2work,
                                  dist2home_wt=dist2home_wt, dist2work_wt=dist2work_wt))

        print('Get records only for top 3 clusters...')
        self.data = pd.merge(self.data, df_top[['uid', 'cluster']], on=['uid', 'cluster'], how='inner')
        print('Calculating activity temporal profiles distance to the templates for top 3...')
        tqdm.pandas()
        df_tempo2dist = self.data.groupby(['uid', 'cluster']).progress_apply(cluster_tempo_distance).reset_index()
        engine = sqlalchemy.create_engine(
            f'postgresql://{self.user}:{self.password}@localhost:{self.port}/{self.db_name}')
        df_tempo2dist.to_sql('tempo_top3_distance', engine, schema='description', index=False, if_exists='append',
                     method='multi', chunksize=5000)


class CommuteODM:
    def __init__(self):
        self.user = preprocess.keys_manager['database']['user']
        self.password = preprocess.keys_manager['database']['password']
        self.port = preprocess.keys_manager['database']['port']
        self.db_name = preprocess.keys_manager['database']['name']
        self.data = None
        self.home_work = None
        self.deso_zones = None
        self.muni_zones = None
        self.home_work_zones = None

    def load_data(self):
        engine = sqlalchemy.create_engine(
            f'postgresql://{self.user}:{self.password}@localhost:{self.port}/{self.db_name}')
        # Original records
        print('Loading stops...')
        self.data = pd.read_sql_query(sql="""SELECT uid, lat, lng, cluster FROM stops_subset;""", con=engine)

        # Spatial zones
        print('Loading spatial zones...')
        self.deso_zones = gpd.GeoDataFrame.from_postgis("SELECT deso, geom FROM public.zones", con=engine).to_crs(4326)
        self.deso_zones = self.deso_zones.rename(columns={'deso': 'zone'})
        self.muni_zones = gpd.read_file(ROOT_dir + '/dbs/municipalities/sweden_komk.shp').to_crs(4326)
        self.muni_zones = self.muni_zones.rename(columns={'KnKod': 'zone'})
        self.muni_zones.zone = self.muni_zones.zone.astype(int)

        # Identified home and work clusters
        print('Loading home and work places...')
        self.home_work = pd.read_sql_query(sql="""SELECT * FROM description.home_work;""", con=engine)
        print(self.home_work.head())
        df_home = self.home_work.loc[:, ['uid', 'home', 'dist2home_wt']].copy()
        df_home = df_home.rename(columns={'home': 'cluster', 'dist2home_wt': 'dist'})
        df_home.loc[:, 'activity'] = 'Home'
        df_work = self.home_work.loc[:, ['uid', 'work', 'dist2work_wt']].copy()
        df_work = df_work.rename(columns={'work': 'cluster', 'dist2work_wt': 'dist'})
        df_work.loc[:, 'activity'] = 'Work'
        self.home_work = pd.concat([df_home, df_work])

        print('Get records only for the home and workplace...')
        self.data = pd.merge(self.data, self.home_work[['uid', 'cluster']], on=['uid', 'cluster'], how='inner')

    def cluster_centroids(self):
        def centroid_calculation(data):
            coords = data.loc[:, ['lng', 'lat']].values
            centroid = (MultiPoint(coords).centroid.x, MultiPoint(coords).centroid.y)  # x='longitude', y='latitude'
            return pd.Series({'lng': centroid[0], 'lat': centroid[1]})
        tqdm.pandas()
        df_centroids = self.data.groupby(['uid', 'cluster']).progress_apply(centroid_calculation).reset_index()
        self.home_work = pd.merge(self.home_work, df_centroids, on=['uid', 'cluster'], how='left')

    def find_zones(self):
        gdf = preprocess.df2gdf_point(self.home_work, 'lng', 'lat', crs=4326, drop=True)
        # Municipality level
        print('Finding municipalities...')
        gdf_muni = gpd.sjoin(gdf, self.muni_zones)
        df_muni = pd.merge(self.home_work, gdf_muni.loc[:, ['uid', 'cluster', 'zone']],
                           on=['uid', 'cluster'], how='left')
        df_muni.loc[:, 'level'] = 'Municipality'

        # DeSO zone level
        print('Finding DeSO zones...')
        gdf_deso = gpd.sjoin(gdf, self.deso_zones)
        df_deso = pd.merge(self.home_work, gdf_deso.loc[:, ['uid', 'cluster', 'zone']],
                           on=['uid', 'cluster'], how='left')
        df_deso.loc[:, 'level'] = 'DeSO'
        self.home_work_zones = pd.concat([df_muni, df_deso])

        print('Saving data...')
        engine = sqlalchemy.create_engine(
            f'postgresql://{self.user}:{self.password}@localhost:{self.port}/{self.db_name}')
        self.home_work_zones.to_sql(f'home_work_zones', engine, schema='description', index=False, if_exists='replace',
                      method='multi', chunksize=5000)


class ODMComparison:
    def __init__(self):
        self.user = preprocess.keys_manager['database']['user']
        self.password = preprocess.keys_manager['database']['password']
        self.port = preprocess.keys_manager['database']['port']
        self.db_name = preprocess.keys_manager['database']['name']
        engine = sqlalchemy.create_engine(
            f'postgresql://{self.user}:{self.password}@localhost:{self.port}/{self.db_name}')
        self.survey_deso = pd.read_csv(os.path.join(ROOT_dir, 'dbs/survey/commute_od_deso.csv'))
        self.survey_muni = pd.read_csv(os.path.join(ROOT_dir, 'dbs/survey/commute_od_municipality.csv'))
        self.survey_muni = self.survey_muni.astype({"ozone": "int", "dzone": "int"})
        self.home_work = pd.read_sql_query(sql="""SELECT * FROM description.home_work_zones;""", con=engine)
        self.deso_pop = pd.read_sql_query(sql="""SELECT deso, befolkning FROM public.zones;""", con=engine) \
            .rename(columns={'deso': 'zone', 'befolkning': 'pop'})

    def distance_thresholds(self):
        df_dist = self.home_work.drop_duplicates(subset=['uid', 'activity'])
        _, bins_h = pd.qcut(df_dist.loc[(df_dist.activity == 'Home') & (~df_dist.dist.isna()), 'dist'], q=10, retbins=True)
        _, bins_w = pd.qcut(df_dist.loc[(df_dist.activity == 'Work') & (~df_dist.dist.isna()), 'dist'], q=10, retbins=True)
        return dict(thre_h=bins_h[1:], thre_w=bins_w[1:])

    def individual_weight(self, df_mad=None):
        # This weight is always at DeSO level
        # df_mad: uid, zone
        df_mad_pop = df_mad.groupby('zone').size().to_frame(name='pop_mad').reset_index()   # zone, pop_mad
        df_mad_pop = pd.merge(df_mad_pop, self.deso_pop, on=['zone'], how='left')
        df_mad_pop.loc[:, 'wt'] = df_mad_pop.loc[:, 'pop'] / df_mad_pop.loc[:, 'pop_mad']   # zone, pop_mad, wt
        df_mad = pd.merge(df_mad, df_mad_pop, on=['zone'], how='left')
        return dict(zip(df_mad.uid, df_mad.wt))

    def mad_odm(self, dist_thre=None, level=None):
        df_dict = dict()
        num_thre = len(dist_thre['thre_h'])
        for qt, thre_h, thre_w in tqdm(zip(range(1, num_thre + 1), dist_thre['thre_h'], dist_thre['thre_w']),
                                       desc='Producing ODM data per distance thresholds'):
            df_h_deso = self.home_work.loc[(self.home_work.activity == 'Home') &
                                      (self.home_work.level == 'DeSO') &
                                      (self.home_work.dist <= thre_h), ['uid', 'zone']]
            uid_weights = self.individual_weight(df_mad=df_h_deso)
            df_h = self.home_work.loc[(self.home_work.activity == 'Home') &
                                      (self.home_work.level == level) &
                                      (self.home_work.dist <= thre_h), ['uid', 'zone']].rename(columns={'zone': 'ozone'})
            df_w = self.home_work.loc[(self.home_work.activity == 'Work') &
                                      (self.home_work.level == level) &
                                      (self.home_work.dist <= thre_w), ['uid', 'zone']].rename(columns={'zone': 'dzone'})
            df = pd.merge(df_h, df_w, on='uid', how='inner')
            df.loc[:, 'wt'] = df.uid.apply(lambda x: uid_weights[x])
            df_dict[qt] = df
        return df_dict

    def odm_aggregation(self, df_dict=None, level=None):
        # Put MAD ODMs and ground truth data together
        # sv_commute, mad_commute
        df_agg_list = []
        for qt, df in df_dict.items():
            df_odm_mad = df.groupby(['ozone', 'dzone'])['wt'].sum().to_frame('mad_commute').reset_index()
            if level == 'DeSO':
                df_agg = pd.merge(self.survey_deso, df_odm_mad, on=['ozone', 'dzone'], how='left').fillna(0)
            else:
                df_odm_mad = df_odm_mad.astype({"ozone": "float", "dzone": "float"})
                df_odm_mad = df_odm_mad.astype({"ozone": "int", "dzone": "int"})
                df_agg = pd.merge(self.survey_muni, df_odm_mad, on=['ozone', 'dzone'], how='left').fillna(0)
                df_agg = df_agg.astype({"ozone": "int", "dzone": "int"})
            df_agg.loc[:, 'qt'] = qt
            df_agg.loc[:, 'level'] = level
            df_agg_list.append(df_agg)
        return pd.concat(df_agg_list)

    def odm_comparison(self, df_odms=None, gt_field=None, mad_field=None):
        def ssi_measure(data):
            data_ = data.copy()
            # Convert trip number to trip frequency (ranging between 0 and 1)
            data_.loc[:, gt_field] = data_.loc[:, gt_field] / data_.loc[:, gt_field].sum()
            data_.loc[:, mad_field] = data_.loc[:, mad_field] / data_.loc[:, mad_field].sum()
            data_.loc[:, 'flow_min'] = data_.apply(lambda row: min(row[gt_field], row[mad_field]), axis=1)
            SSI = 2 * data_.loc[:, 'flow_min'].sum() / \
                  (data_.loc[:, gt_field].sum() + data_.loc[:, mad_field].sum())

            # Keep non-zero for both to compare
            data_non_zero = data.copy()
            data_non_zero = data_non_zero.loc[(data_non_zero[gt_field] != 0) & (data_non_zero[mad_field] != 0)]
            data_non_zero.loc[:, gt_field] = data_non_zero.loc[:, gt_field] / data_non_zero.loc[:, gt_field].sum()
            data_non_zero.loc[:, mad_field] = data_non_zero.loc[:, mad_field] / data_non_zero.loc[:, mad_field].sum()
            data_non_zero.loc[:, 'flow_min'] = data_non_zero.apply(lambda row: min(row[gt_field], row[mad_field]), axis=1)
            SSI_n = 2 * data_non_zero.loc[:, 'flow_min'].sum() / \
                  (data_non_zero.loc[:, gt_field].sum() + data_non_zero.loc[:, mad_field].sum())
            return pd.Series(dict(ssi=SSI, ssi_n=SSI_n))
        tqdm.pandas()
        df_ssi = df_odms.groupby(['level', 'qt']).progress_apply(ssi_measure).reset_index()
        return df_ssi






