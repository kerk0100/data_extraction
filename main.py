import xml.etree.ElementTree as et

import dateutil.parser as parser
import osmnx
import pandas as pd
import requests
from dateutil import tz
from osmnx import geocoder
from shapely.geometry import Point

import os
import sys


# get data from api url and save into element tree
def get_data(dataset_url):
    get_data_from_url = requests.get(dataset_url).content  # get data from source
    root = et.XML(get_data_from_url)  # generate element tree from xml data
    return root


# convert element tree to df
def raw_data_to_df(raw_data):
    dataset = []  # temp list to store rows of data for df
    for element in raw_data:
        for row in element:
            for content in row:
                if 'properties' in content.tag:
                    dataset_row = {}
                    for item in content:
                        if "PartitionKey" in item.tag or "RowKey" in item.tag:  # ignore partition and row columns
                            continue
                        key = item.tag.split("}")[1].lower()  # remove formatting for headers -- something more dynamic would be useful
                        value = item.text
                        dataset_row[key] = value
                    dataset.append(dataset_row)

    data = pd.DataFrame(dataset)
    data = data.apply(lambda x: x.str.strip() if x.dtype == "object" else x)  # strip whitespace
    return data


# check if value can be parsed into a date
def is_date(col, data):
    if data[col].dtype != object:
        return False
    try:
        for i in data[col].values:
            if i:
                parser.parse(i)  # try to parse string into datetime type
                return True
    except ValueError:
        return False


# convert date to local timezone
def date_to_local(date, timezone='America/Nanaimo'):
    date_to_datetime = parser.parse(date)
    local_timezone = tz.gettz(timezone)
    local_time = date_to_datetime.astimezone(local_timezone).isoformat()
    return local_time


# create a local date column from existing date column
def add_local_timezone(data):
    for column in data.columns:
        if is_date(column, data):
            try:
                data['datetime'] = data.apply(lambda x: date_to_local(x[column]), axis=1)
                break  # once datetime column exists, break out of the loop
            except:
                continue


# check if coordinates are within nanaimo limits
def within_city(coordinates, geom):
    coords = (coordinates.y, coordinates.x)
    return geom.intersects(Point(coords))


# clean data by removing duplicates, addressing missing values, and checking if coords are within city
def clean_data(data, primary_key, city='Nanaimo, BC, Canada'):
    formatted_primary_key = primary_key.lower().strip()  # lowercase and remove potential spaces
    data.drop_duplicates(subset=formatted_primary_key, keep='first', inplace=False, ignore_index=False)
    data = data[data[formatted_primary_key].notna()]  # remove rows where primary key = n/a (e.g. license)
    data = data[data[formatted_primary_key].notnull()]  # remove rows where primary key = null (e.g. license)

    # remove rows that are not within city boundaries
    gdf = osmnx.geocoder.geocode_to_gdf(city)  # finds the boundary of city
    geom = gdf.loc[0, 'geometry']
    data = data[data['geometry'].apply(lambda x: within_city(x, geom))]

    # fill missing values
    for column in data:
        if data[column].nunique() == 1:
            data[column] = data[column].fillna(data[column].unique()[0])
    return data


# -------------TASK 1 ------------- #
# dataset_URL = 'https://api.nanaimo.ca/dataservice/v1/sql/BusinessLicences/'
# primary_key = 'licence'

if __name__ == "__main__":
    if len(sys.argv) == 3:
        dataset_URL = sys.argv[1]
        primary_key = sys.argv[2]
    else:
        dataset_URL = input("Enter API URL: ")
        primary_key = input("Enter primary key of dataset: ")

    print(f"Getting data from API: {dataset_URL}")
    raw_data = get_data(dataset_URL)

    print("Formatting Data...")
    data = raw_data_to_df(raw_data)

    # assigning numeric types to appropriate columns
    data = data.apply(pd.to_numeric, errors='ignore')

    # add column for local timezone in ISO-8601 format
    add_local_timezone(data)

    # add geometry column if lat/lon columns exist
    if all(column in data.columns for column in ['latitude', 'longitude']):
        data['geometry'] = [Point(xy) for xy in zip(data.latitude, data.longitude)]

    # data cleaning
    data = clean_data(data, primary_key)

    # save file to csv
    filename = str(dataset_URL.split("/")[-2])
    data.to_csv(filename + '.csv')
    print(f"Data saved to csv file: {filename}.csv")

    # -------------TASK 2 ------------- #
    # Save data in Parquet
    if 'geometry' in data:
        data = data.astype({"geometry": str})  # convert geometry column to string
    data.to_parquet(filename + ".parquet", engine='pyarrow')
    print(f"Data saved to parquet file: {filename}.parquet")

    # -------------TASK 3 ------------- #
    # reduce size of dataset
    print(f"Memory usage before reduction: {round(data.memory_usage(index=True).sum()/1000)} KB")
    data = data.apply(pd.to_numeric, downcast='float', errors='ignore')
    data = data.apply(pd.to_numeric, downcast='integer', errors='ignore')
    data.to_csv("compressed.csv")
    print(f"Memory usage after reduction: {round(data.memory_usage(index=True).sum()/1000)} KB")
    data.to_csv(filename + '_compressed.csv', compression="gzip")
    print(f"Compressed File Size: {round(os.stat(filename + '_compressed.csv').st_size/1000)} KB")

    # -------------TASK 4 ------------- #
    # try different datasets -- enter into command line
    # python main.py http://api.nanaimo.ca/dataservice/v1/sql/Construction/ projectID
    # python main.py http://api.nanaimo.ca/dataservice/v1/sql/DevelopmentApplications/ FolderNumber


