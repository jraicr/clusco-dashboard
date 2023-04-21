import pandas as pd
import datetime as dt
import dask
import multiprocessing
import hvplot.pandas
import holoviews as hv
import hvplot.dask
import panel as pn
import datashader as ds
from pymongo import MongoClient
import holoviews.operation.datashader as hd
from matplotlib.colors import LinearSegmentedColormap


hv.extension('bokeh')
pn.extension(loading_spinner='dots', loading_color='#00aa41',
             sizing_mode="stretch_width")
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
    pandas_df = df.compute()
    dynamic_map = pandas_df.hvplot.line(x=x, y=y,
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
            query = {'name': property_name, 'date': {'$gte': dt.datetime(
                date.year, date.month, date.day), '$lt': dt.datetime(date.year, date.month, date.day) + dt.timedelta(days=1)}}
            print('Retrieving ' + property_name +
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
            date.year, date.month, date.day), '$lt': dt.datetime(date.year, date.month, date.day) + dt.timedelta(days=1)}}

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
        pandas_df_long[var_name] = pandas_df_long[var_name].str.replace(
            var_name+'_', '')

        dask_df_long = dask.dataframe.from_pandas(
            pandas_df_long, npartitions=multiprocessing.cpu_count()).persist()

    else:  # Return a empty dask dataframe in case no data is found
        pandas_df = pd.DataFrame()
        dask_df_long = dask.dataframe.from_pandas(
            pandas_df, npartitions=multiprocessing.cpu_count()).persist()

    return dask_df_long


def empty_plot():
    # draw an empty plot
    empty_plot = hv.Curve([], max_height=400)

    # Add text the plot indicating "There is no available data in the selected date"
    empty_plot = empty_plot * \
        hv.Text(0.6, 0.6, 'There is no available data in the selected date')

    return empty_plot


def plot_data(data, x, y, title, xlabel, ylabel, groupby):
    print("Making plot from data")

    lines_plot = hvplot_dask_df_line(data, x=x, y=y, width=600, height=400, title=title, dic_opts={
        'padding': 0.1, 'tools': ['hover'], 'xlabel': xlabel, 'ylabel': ylabel, 'axiswise': True, 'min_height': 400, 'responsive': True}, groupby=groupby)

    # GRAFICA SCATTER
    # Custom color map
    cmap_custom = LinearSegmentedColormap.from_list('mycmap', [(
        0, (0, 0, 1)), (18/30, (0, 1, 0)), (25/30, (1, 0.65, 0)), (26/30, (1, 0, 0)), (1, (1, 0, 0))])

    single_channel_scatter_plot = hvplot_dask_df_scatter(data, x=x, y=y, width=600, height=400, title=title, color=y, cmap=cmap_custom, size=20, marker='o', dic_opts={
        'padding': 0.1, 'tools': ['hover'], 'xlabel': xlabel, 'ylabel': ylabel, 'clim': (0, 30), 'alpha': 0.5, 'min_height': 400, 'responsive': True}, groupby=groupby)

    # Plot scatter de una hora para TODOS los canales
    all_channels_scatter_plot = hvplot_dask_df_scatter(data, x=x, y=y, width=600, height=400, title=title,
                                                       color=y, cmap=cmap_custom,  size=20, marker='o', dic_opts={'padding': 0.1, 'xlabel': xlabel, 'alpha': 0.30, 'ylabel': ylabel, 'clim': (0, 30)}, rasterize=True, dynamic=False)

    # Juntamos gráfico de lineas y scatters
    composite_plot = lines_plot * single_channel_scatter_plot * all_channels_scatter_plot

    return composite_plot


def manage_plot(mongodb_collection, initial_data, title, start_date, date_picker, property_name, value_field, id_var, var_name, value_name, xlabel, ylabel, remove_temperature_anom, search_previous):

    @pn.depends(date_picker.param.value)
    def update_plot(date_picker):
        print("start_date: " + str(start_date))
        print("date_picker: " + str(date_picker))

        if date_picker != start_date:
            print('getting new dataframe')
            data = get_data_by_date(collection=mongodb_collection,
                                    property_name=property_name,
                                    date_time=date_picker, value_field=value_field,
                                    id_var=id_var, var_name=var_name,
                                    value_name=value_name, remove_temperature_anom=remove_temperature_anom, search_previous=search_previous)
            if len(data.index) != 0:
                print('plotting and adding to grid')
                plot = plot_data(data, id_var, value_name,
                                 title, xlabel, ylabel, var_name)

            else:
                print('No data to print!')
                plot = empty_plot()

        else:
            plot = plot_data(initial_data, id_var, value_name,
                             title, xlabel, ylabel, var_name)

        plot_panel = pn.panel(plot, widget_location='bottom', widgets={
            var_name: pn.widgets.DiscreteSlider}, linked_axes=False)
        return plot_panel

    plot_panel = update_plot

    return plot_panel
    
     
if __name__ == '__main__':
    pn.param.ParamMethod.loading_indicator = True

    # Setup BD Connection
    db = connect_to_database('localhost', 27017, 'CACO')
    clusco_min_collection = db['CLUSCO_min']

    date_filter = dt.date.today()

    # Data retrieved from database
    pacta_temperature_data = get_data_by_date(collection=clusco_min_collection, property_name='scb_pixel_temperature',
                                              date_time=date_filter, value_field='avg', id_var='date', var_name='channel', value_name='temperature', remove_temperature_anom=True)

    scb_temperature_data = get_data_by_date(collection=clusco_min_collection, property_name='scb_temperature',
                                            date_time=date_filter, value_field='avg', id_var='date', var_name='module', value_name='temperature', remove_temperature_anom=False)

    scb_humidity_data = get_data_by_date(collection=clusco_min_collection, property_name='scb_humidity',
                                         date_time=date_filter, value_field='avg', id_var='date', var_name='module', value_name='humidity', remove_temperature_anom=False)

    scb_anode_current_data = get_data_by_date(collection=clusco_min_collection, property_name='scb_pixel_an_current',
                                              date_time=date_filter, value_field='avg', id_var='date', var_name='channel', value_name='anode', remove_temperature_anom=False)

    hv_data = get_data_by_date(collection=clusco_min_collection, property_name='scb_pixel_hv_monitored',
                               date_time=date_filter, value_field='avg', id_var='date', var_name='channel', value_name='hv', remove_temperature_anom=False)

    start_date = pacta_temperature_data.compute()['date'].dt.date[0]

    date_picker = pn.widgets.DatePicker(
        name='Date Selection', value=start_date, end=dt.date.today())

    #pacta_temp_plot_panel = manage_pacta_plot(clusco_min_collection, pacta_temperature_data, start_date, date_picker)
    pacta_temp_plot_panel = manage_plot(clusco_min_collection, pacta_temperature_data, 'PACTA Temperature', start_date,
                                        date_picker, 'scb_pixel_temperature', 'avg', 'date', 'channel', 'temperature', 'Date', 'Temperature (ºC)', True, False)
    scb_temp_plot_panel = manage_plot(clusco_min_collection, scb_temperature_data, 'SCB Temperature', start_date,
                                      date_picker, 'scb_temperature', 'avg', 'date', 'module', 'temperature', 'Date', 'Temperature (ºC)', False, False)
    scb_humidity_plot_panel = manage_plot(clusco_min_collection, scb_humidity_data, 'SCB Humidity', start_date,
                                          date_picker, 'scb_humidity', 'avg', 'date', 'module', 'humidity', 'Date', 'Humidity (%)', False, False)
    scb_anode_current_plot_panel = manage_plot(clusco_min_collection, scb_anode_current_data, 'Anode Current', start_date,
                                               date_picker, 'scb_pixel_an_current', 'avg', 'date', 'channel', 'anode', 'Date', 'Anode Current (µA)', False, False)
    hv_plot_panel = manage_plot(clusco_min_collection, hv_data, 'HV', start_date, date_picker,
                                'scb_pixel_hv_monitored', 'avg', 'date', 'channel', 'hv', 'Date', 'HV (V)', False, False)
    
    # Creamos grid
    grid = pn.GridSpec(sizing_mode='stretch_both',
                       max_height=500, ncols=3, nrows=2)

    # We create two grids in order two group with different columns arrangement
    grid[0, 0] = pacta_temp_plot_panel
    grid[0, 1] = scb_temp_plot_panel
    grid[0, 2] = scb_humidity_plot_panel
    grid[1, 0:2] = scb_anode_current_plot_panel
    grid[1, 2:3] = hv_plot_panel

    bootstrap = pn.template.BootstrapTemplate(title='Clusco Reports')

    bootstrap.main.append(grid)
    bootstrap.sidebar.append(date_picker)

    pn.serve(bootstrap, port=5006, allow_websocket_origin='localhost:5006',
             websocket_origin='localhost:5006', verbose=True, dev=True, num_procs=0, show=False)
