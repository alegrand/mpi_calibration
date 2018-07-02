import pandas
import zipfile
import yaml
import io
import os


def extract_zip(zip_name):
    '''Taken from https://stackoverflow.com/a/10909016/4110059'''
    input_zip = zipfile.ZipFile(zip_name)
    result = {}
    deployment = yaml.load(input_zip.read('info.yaml'))['deployment']
    experiment = zip_name
    if '/' in experiment:
        experiment = experiment[experiment.index('/')+1:]
    experiment = experiment[:experiment.index('_')]
    for name in input_zip.namelist():
        if name.endswith('.csv'):
            dataframe = pandas.read_csv(io.BytesIO(input_zip.read(name)), names = ['op', 'msg_size', 'start', 'duration'])
            dataframe['experiment'] = experiment
            dataframe['type'] = name
            dataframe['deployment'] = deployment
            dataframe['index'] = range(len(dataframe))
            result[name] = dataframe
    return result


def extract_folder(folder_name):
    result = {}
    for root, dirs, files in os.walk(folder_name):
        for file in files:
            if file.endswith('.zip'):
                filename = os.path.join(root, file)
                result[filename] = extract_zip(filename)
    return result


def aggregate_dataframe(dataframe):
    df = dataframe.groupby('msg_size').mean().reset_index()
    df['experiment'] = dataframe['experiment'].unique()[0]
    return df


def lower_quantile(df):
    df = pandas.DataFrame(df)
    quantiles = df.groupby('msg_size').quantile(0.5).reset_index()
    df['above_quantile'] = True
    for size in quantiles.msg_size:
        duration_thresh = quantiles[quantiles.msg_size == size].duration.unique()[0]
        df.loc[(df.msg_size == size) & (df.duration < duration_thresh), 'above_quantile'] = False
    return df[~df.above_quantile]


def clean_dataset(dataframe):
    def aggregate_dataframe(dataframe):
        df = dataframe.groupby('msg_size').mean().reset_index()
        return df

    def lower_quantile(df):
        df = pandas.DataFrame(df)
        quantiles = df.groupby('msg_size').quantile(0.5).reset_index()
        df['above_quantile'] = True
        for size in quantiles.msg_size:
            duration_thresh = quantiles[quantiles.msg_size == size].duration.unique()[0]
            df.loc[(df.msg_size == size) & (df.duration < duration_thresh), 'above_quantile'] = False
        return df[~df.above_quantile]

    return aggregate_dataframe(lower_quantile(dataframe))
