# %%
import pandas as pd
import datetime as dt
import dask
import numpy as np
import hvplot.pandas
import holoviews as hv
import hvplot.dask
from pymongo import MongoClient
import panel as pn
import datashader as ds
import datashader.transfer_functions as tf
from holoviews.operation.datashader import datashade, rasterize
import multiprocessing
from bokeh.models.formatters import DatetimeTickFormatter

# %%
hv.extension('bokeh')
pn.extension()
pd.options.plotting.backend = 'holoviews'

# %%
# Constant limit for query db
HOURS_COUNT = 720 # 30 days 
MINS_COUNT =  1440 # Los últimos 1440 registros de 1 minuto, equivalente a 1 día de datos

# %%
# MongoDB Connection (default localhost if)
print("Connecting to database...")

try:
    client = MongoClient(serverSelectionTimeoutMS = 2000)
    client.server_info()
except:
    print('Connection to database (' + str(client.HOST) + ':' + str(client.PORT) + ') failed.\nCheck if the database is running.')

print('Database connection successful.')
caco_db = client['CACO']
clusco_min_collection = caco_db['CLUSCO_min']
clusco_hour_collection = caco_db['CLUSCO_hour']
    
# Query for all documents with name scb_pixel_temperature present
SCB_pixel_temperature_all_query = {'name': 'scb_pixel_temperature'}

# Query for all documents with a range of dates
SCB_pixel_temperature_current_year_query = {'name': 'scb_pixel_temperature', 'date': {'$gte': dt.datetime(dt.date.today().year, 1, 1), '$lt': dt.datetime.today()}}

# Carga de los datos desde la base de datos a una lista de listas
data_1min = []
dates_1min = []

data_1hour = []
dates_1hour = []

# %%
print('Getting data from DB...')

# Query with all hourly results for the current year and cache dates and averages temperatures from each channel.
for document in clusco_hour_collection.find(SCB_pixel_temperature_current_year_query):
    data_1hour.append(document['avg'])
    dates_1hour.append(document['date'])
    
# Query with all minutes data results for the current year and cache dates and averages temperatures from each channel.
for document in clusco_min_collection.find(SCB_pixel_temperature_current_year_query).limit(MINS_COUNT):
    data_1min.append(document['avg'])
    dates_1min.append(document['date'])

# %%
print('Building pandas dataframe...')

# Pandas dataframe
scb_p_temp_df_1min = pd.DataFrame(data_1min, columns=[f"channel_{i+1}" for i in range(len(data_1min[0]))])
scb_p_temp_df_1hour = pd.DataFrame(data_1hour, columns=[f"channel_{i+1}" for i in range(len(data_1hour[0]))])

# Add dates to dataframe
scb_p_temp_df_1min['date'] = pd.to_datetime(dates_1min)
scb_p_temp_df_1hour['date'] = pd.to_datetime(dates_1hour)
#scb_p_temp_df = scb_p_temp_df.set_index('date')

# Melt dataframe to converts from width df to long,
# where channel would be a variable and the temperature the value...
print('Transforms pandas dataframe from wide to long...')
scb_p_temp_df_long_1min = scb_p_temp_df_1min.melt(id_vars=['date'], var_name='channel', value_name='temperature')
scb_p_temp_df_long_1hour = scb_p_temp_df_1hour.melt(id_vars=['date'], var_name='channel', value_name='temperature')

# Eliminamos valores de 0ºC que probablemente se debe a algún tipo de error al guardar o recoger el dato del sensor
scb_p_temp_df_long_1min = scb_p_temp_df_long_1min[scb_p_temp_df_long_1min.temperature != 0]
scb_p_temp_df_long_1hour = scb_p_temp_df_long_1hour[scb_p_temp_df_long_1hour.temperature != 0]

 # Removes 'avg_' from channel name column
scb_p_temp_df_long_1min['channel'] = scb_p_temp_df_long_1min['channel'].str.replace('channel_', '')
scb_p_temp_df_long_1hour['channel'] = scb_p_temp_df_long_1hour['channel'].str.replace('channel_', '')

# Converts pandas dataframe to dask dataframe
#dask_df = dask.dataframe.from_pandas(scb_p_temp_df_long, npartitions=8)
scb_p_temp_dask_df_1min = dask.dataframe.from_pandas(scb_p_temp_df_long_1min, npartitions=multiprocessing.cpu_count()).persist()
scb_p_temp_dask_df_1hour = dask.dataframe.from_pandas(scb_p_temp_df_long_1hour, npartitions=multiprocessing.cpu_count()).persist()

# %%
scb_p_temp_df_1hour.head()

# %%
scb_p_temp_df_long_1hour.head()


# %%
scb_p_temp_dask_df_1hour

# %%
dates_with_zero_temp = scb_p_temp_df_long_1hour.loc[scb_p_temp_df_long_1hour['temperature'] == 0, 'date']

print(dates_with_zero_temp)

# %%
temperature_channels_line_plot_1hour = scb_p_temp_dask_df_1hour.compute().hvplot.line(x='date', y='temperature', groupby='channel', width=800, height=400, title='Average PACTA Temperature (1 hour resolution)')
temperature_channels_line_plot_1hour.opts(padding=0.1, tools=['hover'], xlabel='Fecha', ylabel='Average Temperature (°C)')

# %%
temperature_channels_line_plot_1min = scb_p_temp_dask_df_1min.compute().hvplot.line(x='date', y='temperature', groupby='channel', width=800, height=400, title='Average PACTA Temperature (minute resolution)')
temperature_channels_line_plot_1min.opts(padding=0.1, tools=['hover'], xlabel='Fecha', ylabel='Average Temperature (°C)')


# %%
pacta_plot_1min = pn.panel(temperature_channels_line_plot_1min)
pacta_plot_1hour = pn.panel(temperature_channels_line_plot_1hour)
# pane2 = pn.panel(pacta_plot)

pacta_plot_1min.servable()
pacta_plot_1hour.servable()


