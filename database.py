"""
Database management module
"""

from pymongo import MongoClient
import pandas as pd
import datetime as dt

def connect(host, port, db_name):
    """
    Connect to a MongoDB database and return a client object from pymongo.
    
    Parameters
    ----------
    - `host`: (string) The host parameter can be a full mongodb URI in addition to a simple hostname or IP.
    - `port` (int) Database port
    - `db_name` (string) The database name

    Returns
    ----------
    - `client`: A client-side representation of a MongoDB cluster from pymongo. See more at <https://pymongo.readthedocs.io/en/3.12.0/api/pymongo/mongo_client.html>
    """
    try:
        client = MongoClient(host=host, port=port,
                             serverSelectionTimeoutMS=5000)
        client.server_info()
    except:
        print(
            f"Connection to database ({host}:{port}) failed.\nCheck if the database is running.")
        return None

    print("Database connection successful.")
    return client[db_name]


def get_data_by_date(collection, property_name, date_time, value_field, id_var, var_name, value_name, search_previous=True):
    """
    Get array data from Mongodb collection filtering by date. If the search_previous flag is set to True, the function will search
    for data in the previous days (until 120 days) if no data is found for the specified date.
    
    Parameters
    ----------
    - `collection` (pymongo.collection.Collection) The collection object from pymongo. See more at <http://pymongo.readthedocs.io/en/3.12.0/api/pymongo/collection.html?highlight=collection#pymongo.collection.Collection>
    - `property_name` (str) The name of the property to search in the collection
    - `date_time` (dt.date) The date to search in the collection. It will search for data from 12:00pm on the selected day until 12:00 the following day.
    - `value_field` (str) The name of the field to retrieve from the collection
    - `id_var` (str) The name of the id variable
    - `var_name` (str) The name of the variable (channel, module)
    - `value_name` (str) The name of the value
    - `search_previous` (bool) Flag to search for data in the previous days if no data is found for the specified date. True by default.

    Returns
    ----------
    - `pandas_df` A pandas dataframe with the data retrieved from the collection, in case no data is found, the function will return and empty dataframe.
    """
    data_values = []
    datetime_values = []

    date = date_time

    if search_previous:
        
        for i in range(0, 120):
            # Query date range from the selected day in date_time parameter from 12:00 pm until the next day at 12:00 pm (inclusive)
            query = {'name': property_name, 'date': {'$gte': dt.datetime(
                date.year, date.month, date.day, 12), '$lte': dt.datetime(date.year, date.month, date.day) + dt.timedelta(days=1, hours=12)}}
            
            print('\nRetrieving ' + property_name +
                  ' data from date: ' + str(date))

            for document in collection.find(query, {"date": 1, value_field: 1, "_id": 0}):
                data_values.append(document[value_field])
                datetime_values.append(document['date'])

            if len(data_values) > 0: # If data is found, break the loop
                break

            else: # Otherwise, search for data in the previous day
                date = date_time - dt.timedelta(days=i+1)
                print('No data found. Retrieving data from previous day...')
    else:
        query = {'name': property_name, 'date': {'$gte': dt.datetime(
                date.year, date.month, date.day, 12), '$lte': dt.datetime(date.year, date.month, date.day) + dt.timedelta(days=1, hours=12)}}

        print('Retrieving ' + property_name +
              ' data from date: ' + str(date))

        for document in collection.find(query, {"date": 1, value_field: 1, "_id": 0}):
            data_values.append(document[value_field])
            datetime_values.append(document['date'])

    if (len(data_values) > 0):

        # Pandas dataframe
        pandas_df = pd.DataFrame(data_values, columns=[
            var_name+f"_{i+1}" for i in range(len(data_values[0]))])

        # Add dates to dataframe and sort by date
        pandas_df['date'] = pd.to_datetime(datetime_values)

        # Melt dataframe to converts from width df to long,
        # where categories such as channel or modules, would be a variable and the temperature the value...
        pandas_df = pandas_df.melt(
            id_vars=[id_var], var_name=var_name, value_name=value_name)

        # Removes 'var_name_' from rows values
        pandas_df[var_name] = pandas_df[var_name].str.replace(
            var_name+'_', '')

        # Converts var name type to int
        pandas_df[var_name] = pandas_df[var_name].astype('uint16', copy=False)
        pandas_df.set_index('date', inplace=True)

        # Sort by date
        pandas_df.sort_index(inplace=True)

        # Print pandas dataframe memory usage to console
        #pandas_df.info(memory_usage='deep')

    else:  # Return a empty pandas dataframe in case no data is found
        pandas_df = pd.DataFrame()

    return pandas_df


def get_scalar_data_by_date(collection, property_name, date_time, value_field, value_name, search_previous=True, remove_zero_values=False):
    """
    Get scalar data from a Mongodb collection filtering by date. If the search_previous flag is set to True, the function will search
    for data in the previous days (until 120 days) if no data is found for the specified date.
    
    Parameters
    ----------
    - `collection` (pymongo.collection.Collection) The collection object from pymongo. See more at <http://pymongo.readthedocs.io/en/3.12.0/api/pymongo/collection.html?highlight=collection#pymongo.collection.Collection>
    - `property_name` (str) The name of the property to search in the collection
    - `date_time` (dt.date) The date to search in the collection. It will search for data from 12:00pm on the selected day until 12:00 the following day.
    - `value_field` (str) The name of the field to retrieve from the collection
    - `value_name` (str) The name of the value
    - `search_previous` (bool) Flag to search for data in the previous days if no data is found for the specified date. True by default.
    - `remove_zero_values` (bool) Boolean flag to remove zero values from the dataframe. False by default.

    Returns
    ----------
    - `pandas_df` A pandas dataframe with the data retrieved from the collection. In case no data is found, the function will return and empty dataframe.

    """
    data_values = []
    datetime_values = []

    date = date_time

    if search_previous:

        for i in range(0, 120):
            # Query date range from the selected day in date_time parameter from 12:00 pm until the next day at 12:00 pm (inclusive)
            query = {'name': property_name, 'date': {'$gte': dt.datetime(
                date.year, date.month, date.day, 12), '$lte': dt.datetime(date.year, date.month, date.day) + dt.timedelta(days=1, hours=12)}}
            


            #query = {'name': property_name, 'date': {'$gte': dt.datetime(
            #    date.year, date.month, date.day), '$lt': dt.datetime(date.year, date.month, date.day) + dt.timedelta(days=1)}}
            print('\nRetrieving ' + property_name +
                  ' data from date: ' + str(date))

            for document in collection.find(query, {"date": 1, value_field: 1, "_id": 0}):
                data_values.append(document[value_field])
                datetime_values.append(document['date'])

            if len(data_values) > 0:
                break

            else:
                date = date_time - dt.timedelta(days=i+1)
                print('No data found. Retrieving data from previous day...')
    else:
        query = {'name': property_name, 'date': {'$gte': dt.datetime(
                date.year, date.month, date.day, 12), '$lte': dt.datetime(date.year, date.month, date.day) + dt.timedelta(days=1, hours=12)}}

        print('Retrieving ' + property_name +
              ' data from date: ' + str(date))

        for document in collection.find(query, {"date": 1, value_field: 1, "_id": 0}):
            data_values.append(document[value_field])
            datetime_values.append(document['date'])

    if (len(data_values) > 0):
        
        # Pandas dataframe
        pandas_df = pd.DataFrame(data_values, columns=[value_name])

        # Add dates to dataframe, set date as index and sort by date
        pandas_df['date'] = pd.to_datetime(datetime_values)
        pandas_df.set_index('date', inplace=True)
        pandas_df.sort_index(inplace=True)

        if remove_zero_values:
            # Remove rows with zero values from dataframe
            pandas_df = pandas_df[pandas_df[value_name] != 0]

        # Print pandas dataframe memory usage to console
        #pandas_df.info(memory_usage='deep')        

    else:  # Return a empty pandas dataframe in case no data is found
        pandas_df = pd.DataFrame()

    return pandas_df