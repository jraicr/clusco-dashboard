from pymongo import MongoClient
import pandas as pd
import datetime as dt

def connect_to_database(host, port, database):
    """
    Connect to a MongoDB database and return a client object.
    """
    try:
        client = MongoClient(host=host, port=port,
                             serverSelectionTimeoutMS=2000)
        client.server_info()
    except:
        print(
            f"Connection to database ({host}:{port}) failed.\nCheck if the database is running.")
        return None

    print("Database connection successful.")
    return client[database]


def get_data_by_date(collection, property_name, date_time, value_field, id_var, var_name, value_name, search_previous=True):
    """_summary_
    Get data from mongodb collection filtering by date. If the search_previous flag is set to True, the function will search
    for data in the previous days (until 120 days) if no data is found for the specified date.
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
        # print('Building pandas dataframe...')

        # Pandas dataframe
        pandas_df = pd.DataFrame(data_values, columns=[
            var_name+f"_{i+1}" for i in range(len(data_values[0]))])

        # Add dates to dataframe and sort by date
        pandas_df['date'] = pd.to_datetime(datetime_values)

        # Melt dataframe to converts from width df to long,
        # where categories such as channel or modules, would be a variable and the temperature the value...
        #print('Transforms pandas dataframe from wide to long...')
        pandas_df = pandas_df.melt(
            id_vars=[id_var], var_name=var_name, value_name=value_name)


        # Removes 'var_name_' from rows values
        pandas_df[var_name] = pandas_df[var_name].str.replace(
            var_name+'_', '')

        # convert var name type to int
        pandas_df[var_name] = pandas_df[var_name].astype('uint16', copy=False)
        pandas_df.set_index('date', inplace=True)

        # sort by date
        pandas_df.sort_index(inplace=True)

        # Print pandas dataframe memory usage to console
        #pandas_df.info(memory_usage='deep')

        # del data_values, datetime_values
        # gc.collect()

    else:  # Return a empty pandas dataframe in case no data is found
        pandas_df = pd.DataFrame()

    return pandas_df