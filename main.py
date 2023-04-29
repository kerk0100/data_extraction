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
import pyarrow


# get data from api url and save into element tree
# XML is a hierarchical data format -- a good way to represent this is in a tree
# there's 2 classes: element tree which maps out the entire XML document and element that maps out just a single
# element (tag) and its sub-elements
def get_data(dataset_url):
    get_data_from_url = requests.get(dataset_url).content  # get data from source
    # the root is the first/outer most tag in the XML document, and all its children are nested within this root
    # create for loops to iterate over each child
    root = et.XML(get_data_from_url)  # generate element tree from xml data
    return root


# convert element tree to df
def raw_data_to_df(raw_data):  # -- feed tag
    dataset = []  # temp list to store rows of data for df
    # Have to get to the sub children
    # print(raw_data)
    for element in raw_data:  # children of the root
        # print(element)
        # -- title, id, updated, link, entry tags
        for row in element:  # grandchildren of the root
            # print(row)
            # -- id, title, updated, author, link, category, content tags
            for content in row:  # grandchildren's kids, where the data we care about is located
                # print(content)
                # -- name, properties tags
                if 'properties' in content.tag:
                    dataset_row = {}
                    for item in content:
                        # print(item)
                        # columns of df
                        if "PartitionKey" in item.tag or "RowKey" in item.tag:  # ignore partition and row columns
                            continue
                        key = item.tag.split("}")[1].lower()  # remove formatting for headers -- something more dynamic would be useful
                        # something with the m: makes the tag include the {url} -- could identify this in some way
                        value = item.text # get the value of that header for the specific row
                        dataset_row[key] = value
                    dataset.append(dataset_row) # append row to list

    data = pd.DataFrame(dataset)  # create df with list of rows (list of dict)
    # if this was cleaner, this step might be in the data cleaning, but it is here to make the date identifying and
    # local time zone easier
    data = data.apply(lambda x: x.str.strip() if x.dtype == "object" else x)  # strip whitespace
    return data


# check if value can be parsed into a date
def is_date(col, data):
    # if the column is not of type object, then return false (filters out the columns that have been converted to
    # numeric in the previous step)
    if data[col].dtype != object:
        return False
    try:
        for i in data[col].values:
            if i:  # if i is a value -- need this conditional statement in case the first value in column is None
                parser.parse(i)  # try to parse string into datetime type
                return True  # if successful, it is a date column and we can continue by converteing to local time
    except ValueError:
        return False # returns false if object column can't be parsed into a date -- e.g. an address


# convert date to local timezone
def date_to_local(date, timezone='America/Vancouver'):
    # receives found date column as input and the timezone is set to Nanaimo (vancouver)
    # this was an error -- 'America/Nanaimo' timezone doesn't exist -- but when you pass None into astimezone()
    # it takes your current timezone -- which is vancouver and the same as nanaimo. If I ran this script in toronto
    # it would have been an incorrect conversion to local time -- would have converted to toronto timezone, not
    # nanaimo
    date_to_datetime = parser.parse(date)  # -- parse the string date into date format
    local_timezone = tz.gettz(timezone)  # set the local timezone to nanaimo (vancouver)
    local_time = date_to_datetime.astimezone(local_timezone).isoformat()  # convert date to local timezone and format
    # in ISO-8601
    return local_time


# create a local date column from existing date column
def add_local_timezone(data):
    # iterate through each column in df -- if date is successfully found and local time is calculated, loop ends.
    # if not, an error is thrown and the loop continues
    for column in data.columns:
        # check if the column values can be parsed into a date, if so, try and convert the dates into local time
        if is_date(column, data):
            try:
                # create a new column named "datetime"
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
    # this section provides the option for the user to enter the inputs directly in the command line,
    # or if those are excluded/they ran it from the IDE, it prompts the user to add the inputs
    # if len(sys.argv) == 3: # file name, URL, primary key
    #     dataset_URL = sys.argv[1]
    #     primary_key = sys.argv[2]
    # else:
    #     dataset_URL = input("Enter API URL: ")
    #     primary_key = input("Enter primary key of dataset: ")

    dataset_URL = 'https://api.nanaimo.ca/dataservice/v1/sql/BusinessLicences/'
    primary_key = 'licence'

    print(f"Getting data from API: {dataset_URL}") # print statement to communicate with user
    raw_data = get_data(dataset_URL)

    print("Formatting Data...")
    data = raw_data_to_df(raw_data)

    # assigning numeric types to appropriate columns
    # this was my way of making the date columns more identifiable
    # if the value was a date, my assumption is that there would be some type of symbol/puncuation/non numeric value in
    # the string. Therefore, the numeric, non-date columns would be switched to the numeric type, while the date column
    # would be left untouched. The way I check for date, is whether the string can be parsed into a date, many numbers
    # can be parsed into date -- ex. 0103 -- I wanted to flag these somehow to not be included
    data = data.apply(pd.to_numeric, errors='ignore')

    # add column for local timezone in ISO-8601 format
    add_local_timezone(data)

    # add geometry column if lat/lon columns exist
    if all(column in data.columns for column in ['latitude', 'longitude']):
        # combines the lat lon to create a new column -- to show point geometries
        # Point is from the Shapely library and it allows easy extraction of x and y coordinates (coord.y = longitude)
        data['geometry'] = [Point(xy) for xy in zip(data.latitude, data.longitude)]  # takes the 2 columns and combines
        # the values in the row to create a new column called geometry

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


