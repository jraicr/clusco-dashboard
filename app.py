import pandas as pd
import datetime as dt
import dask
import multiprocessing
import hvplot.pandas
import holoviews as hv
import hvplot.dask
import panel as pn
import datashader as ds
# import datashader.transfer_functions as tf
from pymongo import MongoClient
import holoviews.operation.datashader as hd
from matplotlib.colors import LinearSegmentedColormap
# import colorcet as cc
# from bokeh.models import DatetimeTickFormatter, HoverTool

hv.extension('bokeh')
pn.extension(loading_spinner='dots', loading_color='#00aa41', sizing_mode="stretch_width")
pd.options.plotting.backend = 'holoviews'



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
        exit()

    print("Database connection successful.")
    return client[database]


def preprocess_data(df, resample_rule='1H'):
    """_summary_
    Preprocess a dataframe by resampling it and filling in missing values.
    """
    df = df.resample(resample_rule).mean()
    df = df.fillna(method='ffill')
    df = df.fillna(method='bfill')
    return df


def hvplot_dask_df_line(df, x, y, width, height, title, dic_opts, color='gray', groupby=None):
    """_summary_
    Plots a dask dataframe line plot.
    """

    dynamic_map = df.compute().hvplot.line(x=x, y=y,
                                           title=title, color=color, groupby=groupby, responsive=True, min_height=400)

    options = list(dic_opts.items())
    dynamic_map.opts(**dict(options))

    return dynamic_map


def hvplot_dask_df_scatter(df, x, y, width, height, title, color, size, marker, dic_opts, cmap="reds", groupby=None, datashade=False, rasterize=False, dynamic=True):
    """_summary_
    Plots a dask dataframe line plot.
    """

    plot = df.compute().hvplot.scatter(x=x, y=y, title=title, color=color,
                                       size=size, marker=marker, cmap=cmap, groupby=groupby, datashade=datashade, rasterize=rasterize, dynamic=dynamic, responsive=True, max_height=400)
    options = list(dic_opts.items())

    plot.opts(**dict(options))
        
    return plot


def get_data_by_date(collection, property_name, date_time, value_field, id_var, var_name, value_name, remove_temperature_anom=False, search_previous=True):

    # query = {'name': property_name, 'date': {'$gte': dt.datetime(dt.date.today().year, dt.date.today().month, dt.date.today().day)}}
    # query = {'name': property_name, 'date': {'$gte': dt.datetime(
    #     date_time.year, date_time.month, date_time.day)}}
    #query = {'name': property_name, 'date': {'$gte': dt.datetime(date.year, date.month, date.day), '$lt': dt.datetime(date.year, date.month, date.day) + dt.timedelta(days=1)}}
    data_values = []
    datetime_values = []

    date = date_time
    
    if search_previous:

        for i in range(0, 120):
            query = {'name': property_name, 'date': {'$gte': dt.datetime(date.year, date.month, date.day), '$lt': dt.datetime(date.year, date.month, date.day) + dt.timedelta(days=1)}}
            print('Retrieving ' + property_name + ' data from date: ' + str(date))

            for document in collection.find(query, {"date": 1, value_field: 1, "_id": 0}):
                data_values.append(document[value_field])
                datetime_values.append(document['date'])

            if len(data_values) > 0:
                break

            else:
                date = date_time - dt.timedelta(days=i+1)
                print('No data found. Retrieving data from previous day...')
    else:
        query = {'name': property_name, 'date': {'$gte': dt.datetime(date.year, date.month, date.day), '$lt': dt.datetime(date.year, date.month, date.day) + dt.timedelta(days=1)}}
        
        for document in collection.find(query, {"date": 1, value_field: 1, "_id": 0}):
                data_values.append(document[value_field])
                datetime_values.append(document['date'])
        
            
    if (len(data_values) > 0):
        print('Building pandas dataframe...')

        # Pandas dataframe
        # pandas_df = pd.DataFrame(data_values, columns=[f"channel_{i+1}" for i in range(len(data_values[0]))])
        pandas_df = pd.DataFrame(data_values, columns=[
                                var_name+f"_{i+1}" for i in range(len(data_values[0]))])

        # Add dates to dataframe
        pandas_df['date'] = pd.to_datetime(datetime_values)

        # Melt dataframe to converts from width df to long,
        # where channel would be a variable and the temperature the value...
        print('Transforms pandas dataframe from wide to long...')
        pandas_df_long = pandas_df.melt(
            id_vars=[id_var], var_name=var_name, value_name=value_name)

        if remove_temperature_anom:
            # Eliminamos valores de 0
            values_field_name_attr = getattr(pandas_df_long, value_name)
            pandas_df_long = pandas_df_long[values_field_name_attr != 0]
            
            # Remove all values below -25
            pandas_df_long = pandas_df_long[values_field_name_attr > -25]
            
            # Remove all values above 250
            pandas_df_long = pandas_df_long[values_field_name_attr < 250]
        

        # Removes 'avg_' from channel name column and convert to dask dataframe
        pandas_df_long['channel'] = pandas_df_long['channel'].str.replace(
            'channel_', '')

        dask_df_long = dask.dataframe.from_pandas(
            pandas_df_long, npartitions=multiprocessing.cpu_count()).persist()

    else: # Return a empty dask dataframe in case no data is found
        pandas_df = pd.DataFrame()
        dask_df_long = dask.dataframe.from_pandas(pandas_df, npartitions=multiprocessing.cpu_count()).persist()
        
    return dask_df_long


def empty_plot():
    # Creamos grid
    grid = pn.GridSpec(sizing_mode='stretch_both',
                       max_height=800, ncols=2, nrows=2)
    # draw an empty plot    
    empty_plot = hv.Curve([], max_height=400)
    
    # Add text the plot indicating "There is no available data in the selected date"
    empty_plot = empty_plot * hv.Text(0.6, 0.6, 'There is no available data in the selected date')
    
    empty_panel_plot = pn.panel(empty_plot, widget_location='bottom_left', widgets={
                                     'channel': pn.widgets.DiscreteSlider}, linked_axes=False)
    
    grid[0, 0] = empty_panel_plot
    
    return grid
    
    
def plot_scb_p_temp(scb_p_temperature_data):
    print("Making plots for SCB pixel temperature")

    # # Query to get temperature channels
    # SCB_pixel_temperature_query = {'name': 'scb_pixel_temperature', 'date': {
    #     '$gte': dt.datetime(dt.date.today().year, 1, 1), '$lt': dt.datetime.today()}}

    # scb_p_temperature_data = get_data(clusco_min_collection, SCB_pixel_temperature_query, 'avg', 'date', 'date', 'channel', 'temperature', 'avg_', '', 1000, wide_to_long=True)
    # scb_p_temperature_data = get_data_by_date(clusco_min_collection, 'scb_pixel_temperature', dt.date.today(), 'avg', 'date', 'channel', 'temperature')

    scb_p_temperature_lines_plot = hvplot_dask_df_line(scb_p_temperature_data, 'date', 'temperature', 600, 400, 'SCB Pixel Temperature', dic_opts={
        'padding': 0.1, 'tools': ['hover'], 'xlabel': 'Date', 'ylabel': 'Temperature (ºC)', 'axiswise': True, 'min_height': 400, 'responsive': True}, groupby='channel')

    # GRAFICA SCATTER
    # Custom color map
    cmap_custom = LinearSegmentedColormap.from_list('mycmap', [(
        0, (0, 0, 1)), (18/30, (0, 1, 0)), (25/30, (1, 0.65, 0)), (26/30, (1, 0, 0)), (1, (1, 0, 0))])

    scb_p_temperature_scatter_plot = hvplot_dask_df_scatter(scb_p_temperature_data, x='date', y='temperature', width=600, height=400, title='SCB Pixel Temperature', color='temperature', cmap=cmap_custom, size=20, marker='o', dic_opts={
        'padding': 0.1, 'tools': ['hover'], 'xlabel': 'Date', 'ylabel': 'Temperature (°C)', 'clim': (0, 30), 'alpha': 0.5, 'min_height': 400, 'responsive': True}, groupby='channel')

    # Plot scatter de una hora para TODOS los canales
    scb_p_temperature_all_channels_scatter_plot = hvplot_dask_df_scatter(scb_p_temperature_data, x='date', y='temperature', width=600, height=400, title='SCB Pixel Temperature',
                                                                               color='temperature', cmap=cmap_custom,  size=20, marker='o', dic_opts={'padding': 0.1, 'xlabel': 'Date', 'alpha': 0.30, 'ylabel': 'Temperature (°C)', 'clim': (0, 30)}, rasterize=True, dynamic=False)
    
    
    # Juntamos gráfico de lineas y scatters
    scb_p_temp_plot = scb_p_temperature_lines_plot * scb_p_temperature_scatter_plot * scb_p_temperature_all_channels_scatter_plot

    # Creamos grid
    grid = pn.GridSpec(sizing_mode='stretch_both',
                       max_height=800, ncols=2, nrows=2)

    
    # Creamos panel con nuestras gráficas y la agregamos al grid. Aquí podemos configurar widgets, etc
    scb_p_temp_plot_panel = pn.panel(scb_p_temp_plot, widget_location='bottom_left', widgets={
                                     'channel': pn.widgets.DiscreteSlider}, linked_axes=False)

    grid[0, 0] = scb_p_temp_plot_panel

    # tabs = pn.Tabs(('Unlinked', grid))
    # return tabs
    return grid


if __name__ == '__main__':
    pn.param.ParamMethod.loading_indicator = True
    
    # Setup BD Connection
    db = connect_to_database('localhost', 27017, 'CACO')

    clusco_min_collection = db['CLUSCO_min']

    date_filter = dt.date.today()
    scb_p_temperature_data = get_data_by_date(collection=clusco_min_collection, property_name='scb_pixel_temperature',
                                              date_time=date_filter, value_field='avg', id_var='date', var_name='channel', value_name='temperature', remove_temperature_anom=True)

    start_date = scb_p_temperature_data.compute()['date'].dt.date[0]
    date_picker = pn.widgets.DatePicker(name='Date Selection', value=start_date, end=dt.date.today())

    #grid = plot_scb_p_temp(scb_p_temperature_data)

    @pn.depends(date_picker.param.value)
    def update_scb_p_plot(date_picker):
            print("start_date: " + str(start_date))
            print("date_picker: " + str(date_picker))
        
            if date_picker != start_date:
                print('getting new dataframe')
                data = get_data_by_date(collection=clusco_min_collection, 
                                                          property_name='scb_pixel_temperature',
                                                          date_time=date_picker, value_field='avg', 
                                                          id_var='date', var_name='channel', 
                                                          value_name='temperature', remove_temperature_anom=True, search_previous=False)
                
                if len(data.index) != 0:
                    print('plotting and adding to grid')
                    grid = plot_scb_p_temp(data)
                else:
                    print('No data to print!')
                    grid = empty_plot()
                
            else:
                grid = plot_scb_p_temp(scb_p_temperature_data)
                
            return grid      



    bootstrap = pn.template.BootstrapTemplate(title='Clusco Reports')


    bootstrap.main.append(update_scb_p_plot)
    bootstrap.sidebar.append(date_picker)

    pn.serve(bootstrap, port=5006, allow_websocket_origin='localhost:5006', websocket_origin='localhost:5006', verbose=True, dev=True, num_procs=0, show=False)
