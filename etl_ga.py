import pandas as pd
import numpy as np
import gc
from pandas.core.common import SettingWithCopyWarning
import warnings
from multiprocessing import Pool as Pool
import functools
import logging


def get_keys_for_field(field=None):
    the_dict = {
        'device': [
            'browser',
            'browserSize',
            'browserVersion',
            'deviceCategory',
            'flashVersion',
            'isMobile',
            'language',
            'mobileDeviceBranding',
            'mobileDeviceInfo',
            'mobileDeviceMarketingName',
            'mobileDeviceModel',
            'mobileInputSelector',
            'operatingSystem',
            'operatingSystemVersion',
            'screenColors',
            'screenResolution'
        ],
        'geoNetwork': [
            'city',
            'cityId',
            'continent',
            'country',
            'latitude',
            'longitude',
            'metro',
            'networkDomain',
            'networkLocation',
            'region',
            'subContinent'
        ],
        'totals': [
            'bounces',
            'hits',
            'newVisits',
            'pageviews',
            'transactionRevenue',
            'visits'
        ],
        'trafficSource': [
            'adContent',
            'adwordsClickInfo',
            'campaign',
            'campaignCode',
            'isTrueDirect',
            'keyword',
            'medium',
            'referralPath',
            'source'
        ],
    }

    return the_dict[field]


def apply_func_on_series(data=None, func=None):
    return data.apply(lambda x: func(x))


def multi_apply_func_on_series(df=None, func=None, n_jobs=4):
    p = Pool(n_jobs)
    f_ = p.map(functools.partial(apply_func_on_series, func=func),
               np.array_split(df, n_jobs))
    f_ = pd.concat(f_, axis=0, ignore_index=True)
    p.close()
    p.join()
    return f_.values


def convert_to_dict(x):
    return eval(x.replace('false', 'False')
                .replace('true', 'True')
                .replace('null', 'np.nan'))


def get_dict_field(x_, key_):
    try:
        return x_[key_]
    except KeyError:
        return np.nan


def develop_json_fields(df=None):
    json_fields = ['device', 'geoNetwork', 'totals', 'trafficSource']
    # Get the keys
    for json_field in json_fields:
        # print('Doing Field {}'.format(json_field))
        # Get json field keys to create columns
        the_keys = get_keys_for_field(json_field)
        # Replace the string by a dict
        # print('Transform string to dict')
        df[json_field] = multi_apply_func_on_series(
            df=df[json_field],
            func=convert_to_dict,
            n_jobs=4
        )

        for k in the_keys:
            # print('Extracting {}'.format(k))
            df[json_field + '.' + k] = df[json_field].apply(lambda x: get_dict_field(x_=x, key_=k))
        del df[json_field]
        gc.collect()
    return df


def main(nrows=None):
    # Convert train
    train = pd.read_csv('./data/train.csv', dtype='object', nrows=None, encoding='utf-8')
    train = develop_json_fields(df=train)
    print('Train Done.')

    # Convert test
    test = pd.read_csv('./data/test.csv', dtype='object', nrows=None, encoding='utf-8')
    test = develop_json_fields(df=test)
    print('Test Done.')

    for f in train.columns:
        if train[f].dtype == 'object':
            train[f] = train[f].apply(lambda x: try_encode(x))
            test[f] = test[f].apply(lambda x: try_encode(x))

    test.to_csv('./data/extracted_fields_test.csv')
    train.to_csv('./data/extracted_fields_train.csv')


def try_encode(x):
    """Used to remove any encoding issues within the data"""
    try:
        return x.encode('utf-8', 'surrogateescape').decode('utf-8')
    except AttributeError:
        return np.nan
    except UnicodeEncodeError:
        return np.nan

if __name__ == '__main__':
    try:
        print('Process Started.')
        main(nrows=None)
        print('Process Ended.')
    except Exception as err:
        logger.exception('Exception occured')
        raise

