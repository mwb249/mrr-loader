import yaml
import os
from arcgis import GIS
import pandas as pd
from datetime import datetime, timedelta


def conn_portal(config):
    """Creates a connection to an ArcGIS Portal."""
    print('Establishing connection to WebGIS...')
    w_gis = None
    try:
        if config['profile']:
            w_gis = GIS(profile=config['profile'])
        else:
            w_gis = GIS(config['portal_url'], config['username'], config['password'])
    except Exception as e:
        print('Error: {}'.format(e))
        print('Exiting script: not able to connect to WebGIS.')
        exit()
    return w_gis


def update_features(config, gis):
    """The update-features function queries a one-to-many related table and load the most recent record attribute data
    into the feature layer table."""

    # Feature fields
    f_flds = config['f_fields']
    f_fld1, f_fld2, f_fld3, f_fld4 = f_flds['f1'], f_flds['f2'], f_flds['f3'], f_flds['f4']
    f_key = f_flds['key']

    # Record fields
    r_flds = config['r_fields']
    r_fld1, r_fld2, r_fld3, r_fld4 = r_flds['f1'], r_flds['f2'], r_flds['f3'], r_flds['f4']
    r_date = r_flds['date']
    r_key = r_flds['key']

    # Construct SQL query (last 30 days of records)
    thirty_days = datetime.today() - timedelta(days=30)
    thirty_days_str = thirty_days.strftime('%Y-%m-%d %H:%M:%S')
    sql = "{} >= DATE '{}'".format(r_date, thirty_days_str)

    # Features layer
    features_lyr = gis.content.get(config['feat_id']).layers[config['feat_lyr_num']]
    features_fset = features_lyr.query()
    features_sdf = features_fset.sdf

    # Records table
    records_tbl = gis.content.get(config['rec_id']).tables[config['rec_tbl_num']]
    records_fset = records_tbl.query(where=sql,
                                     out_fields='{}, {}, {}, {}, {}, {}'.format(r_fld1, r_fld2, r_fld3, r_fld4,
                                                                                r_date, r_key))
    records_sdf = records_fset.sdf
    records_sdf = records_sdf.sort_values(r_date, ascending=False)\
        .drop_duplicates(subset=r_key)

    # Overlapping rows
    overlap_rows = pd.merge(left=features_sdf, right=records_sdf, how='inner', left_on=f_key, right_on=r_key)

    # Update feature attributes
    for key in overlap_rows[f_key]:
        try:
            feature = [f for f in features_fset.features if f.attributes[f_key] == key][0]
            record = [f for f in records_fset.features if f.attributes[r_key] == key][0]
            feature.attributes[f_fld1] = record.attributes[r_fld1]  # Status field
            feature.attributes[f_fld2] = record.attributes[r_fld2]
            feature.attributes[f_fld3] = record.attributes[r_fld3]
            feature.attributes[f_fld4] = record.attributes[r_fld4]
            features_lyr.edit_features(updates=[feature])
            print('Updated {}: {} status to {}'.format(f_key, feature.attributes[f_key], feature.attributes[f_fld1]),
                  flush=True)
        except Exception as e:
            print('Exception: {}'.format(e))
            continue


if __name__ == '__main__':
    # Set current working directory
    cwd = os.getcwd()

    # Open config file
    with open(cwd + '/config.yml', 'r') as yaml_file:
        cfg = yaml.load(yaml_file, Loader=yaml.FullLoader)

    # Create a connection to an ArcGIS Portal
    webgis_conn = conn_portal(cfg)

    # Update features with most recent record data
    update_features(cfg, webgis_conn)
