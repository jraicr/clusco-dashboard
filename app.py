import pandas as pd
import datetime as dt
import dask
import multiprocessing
import hvplot.pandas
import holoviews as hv
import hvplot.dask
import panel as pn
import datashader as ds
#import datashader.transfer_functions as tf
from pymongo import MongoClient
#from holoviews.operation.datashader import datashade, rasterize
from matplotlib.colors import LinearSegmentedColormap
#import colorcet as cc
#from bokeh.models import DatetimeTickFormatter, HoverTool

hv.extension('bokeh')
pn.extension()
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


def query_database(collection, query, valuesPropertyName, datesPropertyName, limit=None):
    """
    Query a MongoDB collection and return data values and dates.
    """
    data = []
    dates = []

    cursor = collection.find(query)

    if limit is not None:
        cursor = cursor.limit(limit)

    for document in cursor:
        data.append(document[valuesPropertyName])
        dates.append(document[datesPropertyName])

    # Add values to dataframe
    new_df = pd.DataFrame(
        data, columns=[valuesPropertyName+f"_{i+1}" for i in range(len(data[0]))])

    # Add dates to the dataframe
    new_df['date'] = pd.to_datetime(dates)

    return new_df


def df_wide_to_long_data(df, id_vars, var_name, value_name):
    """_summary_
    Melt dataframe to converts from width df to long.
    """
    df_long = df.melt(id_vars=[id_vars],
                      var_name=var_name, value_name=value_name)

    return df_long


def remove_zero_values(df, df_key_column):
    """_summary_
    Removes zero values from a dataframe.
    """
    return df[df_key_column != 0]


def replace_column_name(df, df_column_name, str_search, str_replace):
    """_summary_
    Replaces a string in a column name.
    """
    return df[df_column_name].str.replace(str_search, str_replace)


def preprocess_data(df, resample_rule='1H'):
    """_summary_
    Preprocess a dataframe by resampling it and filling in missing values.
    """
    df = df.resample(resample_rule).mean()
    df = df.fillna(method='ffill')
    df = df.fillna(method='bfill')
    return df


def convert_to_dask_df(df, npartitions=multiprocessing.cpu_count()):
    """_summary_
    Converts a pandas dataframe to a dask dataframe.
    """
    return dask.dataframe.from_pandas(df, npartitions=npartitions).persist()


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


def create_dashboard(*plots):
    """_summary_
    Creates a panel grid and add panels i different positions
    """
    grid = pn.GridSpec(sizing_mode='stretch_both',
                       max_height=800, ncols=1, nrows=len(plots))
    for i, plot in enumerate(plots):
        panel = pn.panel(plot, widget_location='bottom_left', widgets={
                         'channel': pn.widgets.DiscreteSlider})
        grid[i, 0] = panel
    return grid

# def create_dashboard(*plots):
#     # Crear cuadrícula de paneles y agregar paneles en diferentes posiciones
#     grid = pn.GridSpec(sizing_mode='stretch_both', max_height=800, ncols=len(plots), nrows=1)
#     for i, plot in enumerate(plots):
#         panel = pn.panel(plot)
#         grid[0, i] = panel
#     return grid


def get_data(collection, query, valuesPropertyName, datesPropertyName, id_vars, var_name, value_names, str_search, str_replace, limit=None, remove_zeros=True, wide_to_long=False):
    '''
    Summary: Get data from the database with a query that get values and dates from the collection.
    Then transform it to long format and remove zero values from the values. 
    Finally, rename column names converted to label row data from data_1, data_2, . ... , data_i to 1, 2 ... , i and return a dask dataframe

    '''
    print('Getting data from database collection ' + collection.name + '...')
    # query = {"date": {"$gte": dt.datetime(2021, 1, 1)}}
    data = query_database(
        collection, query, valuesPropertyName, datesPropertyName, limit)

    if wide_to_long:
        print('Converting wide dataframe to long...')        # Convert data to long format
        data = df_wide_to_long_data(
            data, id_vars, var_name, value_names)

        # Rename channel names from avg_1, avg_2, . ... , avg_i to 1, 2 ... , i
        data[var_name] = replace_column_name(
            data, var_name, str_search, str_replace)

        if remove_zeros:
            print('Removing zero values from ' + value_names + ' field...') 
            # Removes zero values from the temperature values
            field = getattr(data, value_names)
            data = remove_zero_values(data, field)
            # data = remove_zero_values(
            #     data, data.temperature)

    # Convert pandas data frame to dask dataframe and returns it
    return convert_to_dask_df(data)

    
def plot_scb_p_temp(clusco_hour_collection, clusco_min_collection, ):
    print("Making plots for average SCB pixel temperature")
    
    # Query to get temperature channels from the current year
    SCB_pixel_temperature_current_year_query = {'name': 'scb_pixel_temperature', 'date': {
        '$gte': dt.datetime(dt.date.today().year, 1, 1), '$lt': dt.datetime.today()}}

    # Query to get temperature channels from the last 1000 values
    SCB_pixel_temperature_last_two_days_query = {'name': 'scb_pixel_temperature', 'date': {
        '$gte': dt.datetime(dt.date.today().year, 1, 1), '$lt': dt.datetime.today()}}

    scb_p_temperature_1h_data = get_data(
        clusco_hour_collection, SCB_pixel_temperature_current_year_query, 'avg', 'date', 'date', 'channel', 'temperature', 'avg_', '', wide_to_long=True)

    scb_p_temperature_2days_data = get_data(
        clusco_min_collection, SCB_pixel_temperature_last_two_days_query, 'avg', 'date', 'date', 'channel', 'temperature', 'avg_', '', 1000, wide_to_long=True)

    # Plot line de una hora para el canal seleccionado
    scb_p_temperature_1h_lines_plot = hvplot_dask_df_line(scb_p_temperature_1h_data, 'date', 'temperature', 600, 400, 'Average SCB Pixel Temperature (2023 - 1 hour resolution)', dic_opts={
                                                          'padding': 0.1, 'tools': ['hover'], 'xlabel': 'Fecha', 'ylabel': 'Temperature (ºC)', 'axiswise': True, 'min_height':400, 'responsive':True}, groupby='channel')

    scb_p_temperature_2days_lines_plot = hvplot_dask_df_line(scb_p_temperature_2days_data, 'date', 'temperature', 600, 400, 'Average SCB Pixel Temperature (Last 1000 values - 1 minute resolution)', dic_opts={
        'padding': 0.1, 'tools': ['hover'], 'xlabel': 'Fecha', 'ylabel': 'Temperature (ºC)', 'axiswise': True, 'min_height':400, 'responsive':True}, groupby='channel')

    # GRAFICA SCATTER
    # Custom color map
    cmap_custom = LinearSegmentedColormap.from_list('mycmap', [(
        0, (0, 0, 1)), (18/30, (0, 1, 0)), (25/30, (1, 0.65, 0)), (26/30, (1, 0, 0)), (1, (1, 0, 0))])

    # Plot scatter de una hora para el canal seleccionado
    scb_p_temperature_1h_scatter_plot = hvplot_dask_df_scatter(scb_p_temperature_1h_data, x='date', y='temperature', width=600, height=400, title='Average SCB Pixel Temperature (1 hour resolution)', color='temperature', cmap=cmap_custom, size=20, marker='o', dic_opts={
        'padding': 0.1, 'tools': ['hover'], 'xlabel': 'Fecha', 'ylabel': 'Average Temperature (°C)', 'clim': (0, 30), 'alpha': 0.5, 'min_height':400, 'responsive':True}, groupby='channel')

    
    # Plot scatter de una hora para TODOS los canales
    scb_p_temperature_1h_all_channels_scatter_plot = hvplot_dask_df_scatter(scb_p_temperature_1h_data, x='date', y='temperature', width=600, height=400, title='Average SCB Pixel Temperature (1 hour resolution)',
                                                                            color='temperature', cmap=cmap_custom,  size=20, marker='o', dic_opts={'padding': 0.1, 'xlabel': 'Fecha', 'alpha': 0.30, 'ylabel': 'Temperature (°C)', 'clim': (0, 30), 'min_height':400, 'responsive':True}, rasterize=True, dynamic=False)

    scb_p_temperature_2days_scatter_plot = hvplot_dask_df_scatter(scb_p_temperature_2days_data, x='date', y='temperature', width=600, height=400, title='Average PACTA Temperature (Last 1000 values - 1 minute resolution)', color='temperature', cmap=cmap_custom, size=20, marker='o', dic_opts={
        'padding': 0.1, 'tools': ['hover'], 'xlabel': 'Fecha', 'ylabel': 'Average Temperature (°C)', 'clim': (0, 30), 'alpha': 0.5, 'min_height':400, 'responsive':True}, groupby='channel')

    # Plot scatter de una hora para TODOS los canales
    scb_p_temperature_2days_all_channels_scatter_plot = hvplot_dask_df_scatter(scb_p_temperature_2days_data, x='date', y='temperature', width=600, height=400, title='Average SCB Pixel Temperature (Last 1000 values - 1 minute resolution)',
                                                                               color='temperature', cmap=cmap_custom,  size=20, marker='o', dic_opts={'padding': 0.1, 'xlabel': 'Fecha', 'alpha': 0.30, 'ylabel': 'Temperature (°C)', 'clim': (0, 30) }, rasterize=True, dynamic=False)
    # Juntamos gráfico de lineas y scatters
    scb_p_temp_1h_plot = scb_p_temperature_1h_lines_plot * \
        scb_p_temperature_1h_scatter_plot * scb_p_temperature_1h_all_channels_scatter_plot

    scb_p_temp_2days_plot = scb_p_temperature_2days_lines_plot * \
        scb_p_temperature_2days_scatter_plot * \
        scb_p_temperature_2days_all_channels_scatter_plot
       
    # Creamos grid
    unlinked_grid = pn.GridSpec(sizing_mode='stretch_both',
                                max_height=800, ncols=2, nrows=2)

    linked_grid = pn.GridSpec(sizing_mode='stretch_both',
                              max_height=800, ncols=2, nrows=2)

    # Creamos panel con nuestras gráficas y la agregamos al grid. Aquí podemos configurar widgets, etc
    scb_p_temp_1h_plot_panel = pn.panel(scb_p_temp_1h_plot, widget_location='bottom_left', widgets={
                                        'channel': pn.widgets.DiscreteSlider}, linked_axes=False)

    scb_p_temp_2days_plot_panel = pn.panel(scb_p_temp_2days_plot, widget_location='bottom_left', widgets={
                                           'channel': pn.widgets.DiscreteSlider}, linked_axes=False)

    scb_p_temp_1h_plot_linked_panel = pn.panel(scb_p_temp_1h_plot, widget_location='bottom_left', widgets={
        'channel': pn.widgets.DiscreteSlider})

    scb_p_temp_2days_plot_linked_panel = pn.panel(scb_p_temp_2days_plot, widget_location='bottom_left', widgets={
        'channel': pn.widgets.DiscreteSlider})

    #scb_p_temp_1h_plot_all_channels_panel = pn.panel(scb_p_temperature_1h_lines_all_channels_plot, widget_location='bottom_left')

    unlinked_grid[0, 0] = scb_p_temp_1h_plot_panel
    unlinked_grid[0, 1] = scb_p_temp_2days_plot_panel
    #unlinked_grid[1, 0] = scb_p_temp_1h_plot_all_channels_panel

    linked_grid[0, 0] = scb_p_temp_1h_plot_linked_panel
    linked_grid[0, 1] = scb_p_temp_2days_plot_linked_panel
    
    tabs = pn.Tabs(('Unlinked', unlinked_grid), ('Linked', linked_grid))
    
    return tabs
    

if __name__ == '__main__':
    # Conectar a la base de datos
    db = connect_to_database('localhost', 27017, 'CACO')
    clusco_hour_collection = db['CLUSCO_hour']
    clusco_min_collection = db['CLUSCO_min']

    
    scb_p_temp_plot_tabs = plot_scb_p_temp(clusco_hour_collection, clusco_min_collection)
    bootstrap = pn.template.BootstrapTemplate(title='Clusco Reports')

    bootstrap.main.append(scb_p_temp_plot_tabs)

    #pn.serve(create_dashboard(scb_p_temp_1h_plot), show=True, port=5006, dev=True, websocket_origin='localhost:5006')
    pn.serve(bootstrap, show=True, port=5006, dev=True, websocket_origin='localhost:5006')
    
