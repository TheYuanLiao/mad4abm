import pandas as pd
import geopandas as gpd
from geoalchemy2 import Geometry, WKTElement
import os
import subprocess
import numpy as np
import osmnx as ox
import yaml
import sqlalchemy
import time
from tqdm import tqdm
from geopandas import GeoDataFrame
from shapely.geometry import Point
from math import radians, cos, sin, asin, sqrt


def get_repo_root():
    """Get the root directory of the repo."""
    dir_in_repo = os.path.dirname(os.path.abspath('__file__'))
    return subprocess.check_output('git rev-parse --show-toplevel'.split(),
                                   cwd=dir_in_repo,
                                   universal_newlines=True).rstrip()


ROOT_dir = get_repo_root()
with open(os.path.join(ROOT_dir, 'dbs', 'keys.yaml')) as f:
    keys_manager = yaml.load(f, Loader=yaml.FullLoader)


def raw2df2db(file, user, password, port, db_name, table_name, schema_name):
    """
    Read the compressed files and get the dataframe for further processing.
    :param file: a string that points to raw records, e.g., "VGR_raw_mobile_2019_10.csv.gz"
    :return:
    A dataframe with each row a request record.
    """
    raw_folder = os.path.join(ROOT_dir, "dbs", "original_input_data")
    chunk_container = pd.read_csv(os.path.join(raw_folder, file + ".gz"), sep=',',
                                  header=0,
                                  compression='gzip',
                                  chunksize=2000000,
                                  error_bad_lines=False)
    chunk_list = []
    for chunk in chunk_container:
        chunk_list.append(chunk)
    df = pd.concat(chunk_list)
    print('Rows number', len(df))
    dump2db_df(df, user, password, port, db_name, table_name, schema_name)
    return df


def raw2chunk2db(file, user, password, port, db_name, table_name, schema_name):
    """
    Read the compressed files and get the chunk for dumping.
    :param file: a string that points to raw records, e.g., "VGR_raw_mobile_2019_10.csv.gz"
    :return:
    A dataframe with each row a request record.
    """
    engine = sqlalchemy.create_engine(f'postgresql://{user}:{password}@localhost:{port}/{db_name}')
    raw_folder = os.path.join(ROOT_dir, "dbs", "original_input_data")
    chunk_container = pd.read_csv(os.path.join(raw_folder, file + ".gz"), sep=',',
                                  header=0, iterator=True,
                                  compression='gzip',
                                  chunksize=2000000,
                                  error_bad_lines=False)
    start = time.time()
    for chunk in tqdm(chunk_container, desc='Dumping data to database by chunk'):
        df = chunk.rename(columns={c: c.replace(' ', '') for c in chunk.columns})
        df.to_sql(table_name, engine, schema=schema_name, index=False, if_exists='append', method='multi', chunksize=5000)
    end = time.time()
    print(end - start)
    return df


def osm_net_retrieve(bbox, network_type, folder='dbs\\geo'):
    """
    Save two formats of network downloaded from OpenStreetMap (.graphml & .shp)
    :param folder: where to save the downloaded data
    :param bbox: bounding box for retrieving the network
    :param network_type: "walk" or "drive"
    :param osm_folder: where to save the network
    :return: None
    """
    north, south, east, west = bbox
    G = ox.graph_from_bbox(north, south, east, west, network_type=network_type)
    ox.save_graphml(G,  filepath=os.path.join(ROOT_dir, folder, network_type + '_network.graphml'))
    gdf = ox.graph_to_gdfs(G)
    edge = gdf[1]
    edge = edge.loc[:, ['geometry', 'highway', 'junction', 'length', 'maxspeed', 'name', 'oneway',
                        'osmid', 'u', 'v', 'width']]

    fields = ['highway', 'junction', 'length', 'maxspeed', 'name', 'oneway',
              'osmid', 'u', 'v', 'width']
    df_inter = pd.DataFrame()
    for f in fields:
        df_inter[f] = edge[f].astype(str)
    gdf_edge = gpd.GeoDataFrame(df_inter, geometry=edge["geometry"])
    gdf_edge.to_file(os.path.join(ROOT_dir, folder, network_type + '_network.shp'))


def dump2db_df(df, user, password, port, db_name, table_name, schema_name):
    """
    Dump a pandas dataframe to a database: a fast solution.
    :param df: string, data to dump as a table
    :param user: string, user name of the target database
    :param password: string, user password of the target database
    :param port: string, port number of the target database
    :param db_name: string, database name
    :param table_name: string, table name to be created in database
    :param schema_name: string, existing schema to create the dataframe as a table
    :return: None
    """
    engine = sqlalchemy.create_engine(f'postgresql://{user}:{password}@localhost:{port}/{db_name}')
    # df.head(0).to_sql(table_name, engine, schema=schema_name, if_exists='replace', index=False)
    # conn = engine.raw_connection()
    # cur = conn.cursor()
    # output = io.StringIO()
    # df.to_csv(output, sep='\t', header=False, index=False)
    # output.seek(0)
    # cur.copy_from(output, "\"" + "\".\"".join([schema_name, table_name]) + "\"", null="")  # null values become ''
    # conn.commit()
    df.to_sql(table_name, engine, schema=schema_name, index=False,
              method='multi', if_exists='replace', chunksize=10000)


def dump2db_gdf(gdf, sdtype, crs, user, password, port, db_name, table_name, schema_name):
    """
    Dump a pandas dataframe to a database: a fast solution.
    :param gdf: string, data to dump as a table
    :param sdtype: string, 'POINT', 'POLYGON' etc
    :param crs: integer, crs number e.g., 4326
    :param crs: string, data to dump as a table
    :param user: string, user name of the target database
    :param password: string, user password of the target database
    :param port: string, port number of the target database
    :param db_name: string, database name
    :param table_name: string, table name to be created in database
    :param schema_name: string, existing schema to create the dataframe as a table
    :return: None
    """
    engine = sqlalchemy.create_engine(f'postgresql://{user}:{password}@localhost:{port}/{db_name}')
    stt = time.time()
    gdf['geom'] = gdf['geometry'].apply(lambda x: WKTElement(x.wkt, srid=crs))

    # drop the geometry column as it is now duplicative
    gdf.drop('geometry', 1, inplace=True)
    gdf.to_sql(
        schema=schema_name,
        name=table_name,
        con=engine,
        if_exists='replace',
        index=False,
        dtype={
            'geom': Geometry(geometry_type=sdtype, srid=crs),
        }
    )
    print('Data dumped: %g' % (time.time() - stt))


def df2gdf_point(df, x_field, y_field, crs=4326, drop=True):
    """
    Convert two columns of GPS coordinates into POINT geo dataframe
    :param drop: boolean, if true, x and y columns will be dropped
    :param df: dataframe, containing X and Y
    :param x_field: string, col name of X
    :param y_field: string, col name of Y
    :param crs: int, epsg code
    :return: a geo dataframe with geometry of POINT
    """
    geometry = [Point(xy) for xy in zip(df[x_field], df[y_field])]
    if drop:
        gdf = GeoDataFrame(df.drop(columns=[x_field, y_field]), geometry=geometry)
    else:
        gdf = GeoDataFrame(df, crs=crs, geometry=geometry)
    gdf.set_crs(epsg=crs, inplace=True)
    return gdf


def haversine(lon1, lat1, lon2, lat2):
    """
    Calculate the great circle distance between two points
    on the earth (specified in decimal degrees) in km
    """
    # convert decimal degrees to radians
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

    # haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    r = 6371 # Radius of earth in kilometers. Use 3956 for miles
    return c * r


def haversine_vec(data):
    """
    Take array of zones' centroids to return the Haversine distance matrix
    :param data: 2d array e.g., list(zones.loc[:, ["Y", "X"]].values)
    :return: a matrix of distance
    """
    # Convert to radians
    data = np.deg2rad(data)

    # Extract col-1 and 2 as latitudes and longitudes
    lat = data[:, 0]
    lng = data[:, 1]

    # Elementwise differentiations for latitudes & longitudes
    diff_lat = lat[:, None] - lat
    diff_lng = lng[:, None] - lng

    # Finally Calculate haversine
    d = np.sin(diff_lat / 2) ** 2 + np.cos(lat[:, None]) * np.cos(lat) * np.sin(diff_lng / 2) ** 2
    return 2 * 6371 * np.arcsin(np.sqrt(d))


def df_col2batches(df, column_name, chunk_size=30000):
    id_list = []
    num_chunks = len(df) // chunk_size + 1
    for i in range(num_chunks):
        id_batch = tuple(df.loc[i * chunk_size:(i + 1) * chunk_size, column_name])
        id_list.append(id_batch)
    return id_list


def df2batches(df, chunk_size=30000):
    df_list = []
    num_chunks = len(df) // chunk_size + 1
    for i in range(num_chunks):
        df_batch = df.loc[i * chunk_size:(i + 1) * chunk_size, :]
        df_list.append(df_batch)
    return df_list