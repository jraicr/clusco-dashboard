import pandas as pd
import datetime as dt
import multiprocessing
import hvplot.pandas
import holoviews as hv
import hvplot.pandas
import panel as pn
import datashader as ds
from pymongo import MongoClient
import holoviews.operation.datashader as hd
from matplotlib.colors import LinearSegmentedColormap
import time
import gc

gc.enable()
#gc.set_debug(gc.DEBUG_STATS)

hv.extension('bokeh', logo=False)
pn.extension(loading_spinner='dots', loading_color='#00204e', sizing_mode="stretch_width")
pd.options.plotting.backend = 'holoviews'

def disable_logo(plot, element):
    """
    Disables blokeh logo in plots
    """
    plot.state.toolbar.logo = None
    

panels_dict = {
  "scb_pixel_temperature": "empty",
  "scb_temperature": "empty",
  "scb_humidity": "empty",
  "scb_pixel_an_current": "empty",
  "scb_pixel_hv_monitored": "empty"
}



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


def hvplot_df_line(df, x, y, title, dic_opts, color='gray', groupby=None):
    """_summary_
    Plots a pandas dataframe line plot.
    """
    if groupby != None:
        dynamic_map = df.hvplot.line(x=x, y=y, title=title, color=color, groupby=groupby, responsive=True, min_height=400, hover_cols=[y, x, groupby])
    else:
        dynamic_map = df.hvplot.line(x=x, y=y, title=title, color=color, groupby=groupby, responsive=True, min_height=400, hover_cols='all')

    options = list(dic_opts.items())
    dynamic_map.opts(**dict(options))

    return dynamic_map

def hvplot_df_max_min_avg_line(df, x, y, title, dic_opts):
    """_summary_
    Plots a pandas dataframe line plot with max, min and average columns in df.
    # TODO: Refactorize this to use only one function for all line plots.
    """

    dynamic_map = df.hvplot.line(x=x, y=['max', 'min', 'avg'], title=title, responsive=True, min_height=400, hover_cols=['x', 'y', 'Variable'])
   

    options = list(dic_opts.items())
    dynamic_map.opts(**dict(options))

    return dynamic_map


def hvplot_df_scatter(df, x, y, title, color, size, marker, dic_opts, cmap="reds", groupby=None, datashade=False, rasterize=False, dynamic=True):
    """_summary_
    Plot a scatter graph from a pandas dataframe
    """
    if rasterize == False:
        plot = df.hvplot.scatter(x=x, y=y, title=title, color=color,
                                   size=size, marker=marker, cmap=cmap, groupby=groupby, datashade=datashade, rasterize=rasterize, dynamic=dynamic, responsive=True, min_height=400)
        
    else:
       plot = df.hvplot.scatter(x=x, y=y, title=title, color=color,
                                   marker=marker, cmap=cmap, groupby=groupby, datashade=datashade, rasterize=rasterize, dynamic=dynamic, responsive=True, min_height=400)
        
    options = list(dic_opts.items())

    plot.opts(**dict(options))

    return plot


def get_data_by_date(collection, property_name, date_time, value_field, id_var, var_name, value_name, remove_temperature_anom=False, search_previous=True):
    """_summary_
    Get data from mongodb collection filtering by date. If the search_previous flag is set to True, the function will search
    for data in the previous days (until 120 days) if no data is found for the specified date.
    """
    data_values = []
    datetime_values = []

    date = date_time

    if search_previous:

        for i in range(0, 120):
            query = {'name': property_name, 'date': {'$gte': dt.datetime(
                date.year, date.month, date.day), '$lt': dt.datetime(date.year, date.month, date.day) + dt.timedelta(days=1)}}
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
            date.year, date.month, date.day), '$lt': dt.datetime(date.year, date.month, date.day) + dt.timedelta(days=1)}}

        print('Retrieving ' + property_name +
                  ' data from date: ' + str(date))
        
        for document in collection.find(query, {"date": 1, value_field: 1, "_id": 0}):
            data_values.append(document[value_field])
            datetime_values.append(document['date'])

    if (len(data_values) > 0):
        print('Building pandas dataframe...')

        # Pandas dataframe
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
            # Removes 0 values
            values_field_name_attr = getattr(pandas_df_long, value_name)
            pandas_df_long = pandas_df_long[values_field_name_attr != 0]

            # Remove all values below -25
            pandas_df_long = pandas_df_long[values_field_name_attr > -25]

            # Remove all values above 250
            pandas_df_long = pandas_df_long[values_field_name_attr < 250]

        print("removing 'channel_' from channel rows values...")
        print(pandas_df_long)
        # Removes 'channel_' from channel rows values
        pandas_df_long[var_name] = pandas_df_long[var_name].str.replace(
            var_name+'_', '')

        print(pandas_df_long)
        pandas_df_result = pandas_df_long
        
        del pandas_df, pandas_df_long, data_values, datetime_values
        gc.collect()

    else:  # Return a empty pandas dataframe in case no data is found
        pandas_df = pd.DataFrame()
        pandas_df_result = pandas_df

        
    return pandas_df_result


def empty_plot():
    # Creates and return an empty plot
    empty_plot = hv.Curve([])

    # Add text the plot indicating "There is no available data in the selected date"
    empty_plot = empty_plot * \
        hv.Text(0.6, 0.6, 'There is no available data in the selected date')

    return empty_plot


def build_dataframe_with_min_max(df, x, y):
   # Build a pandas dataframe from the original dataframe and select the min and max values for each date
    new_df = df.groupby(x)[y].agg(['min', 'max', 'mean'])
    new_df = new_df.reset_index()
    #new_df = new_df.rename(columns={'min': 'min_'+y, 'max': 'max_'+y, 'mean': 'avg_'+y})
    new_df = new_df.rename(columns={'mean': 'avg'})
    return new_df
    

def plot_data(data, x, y, title, xlabel, ylabel, groupby, cmap_custom, clim):
    
    # Build a pandas dataframe from the original dataframe and select the min and max values for each date corresponding to each group
    
    df_with_min_max_avg = build_dataframe_with_min_max(data, x, y)    
    
    print("   - Creating plot for: " +  title)    
    
    max_line_plot = hvplot_df_max_min_avg_line(df_with_min_max_avg, x=x, y=y, title=title, dic_opts={
        'padding': 0.1, 'tools': ['hover'], 'xlabel': xlabel, 'ylabel': ylabel, 'axiswise': True, 'show_legend':True})
    
    # Plot lines grouped by channel from data (channel is selected by widget and just one channel is shown at a time)
    lines_plot = hvplot_df_line(data, x=x, y=y,title=title, dic_opts={
        'padding': 0.1, 'tools': ['hover'], 'xlabel': xlabel, 'ylabel': ylabel, 'axiswise': True}, groupby=groupby)


    # Plot scatter from data for a single channel
    single_channel_scatter_plot = hvplot_df_scatter(data, x=x, y=y, title=title, color=y, cmap=cmap_custom, size=15, marker='o', dic_opts={
        'padding': 0.1, 'tools': [''], 'xlabel': xlabel, 'ylabel': ylabel, 'clim': clim, 'alpha': 0.5}, groupby=groupby)

    # Plot scatter from data for all channels (rasterized)
    all_channels_scatter_plot = hvplot_df_scatter(data, x=x, y=y, title=title, color=y, cmap=cmap_custom,  size=20, marker='o', dic_opts={'padding': 0.1, 'tools': [''], 'xlabel': xlabel, 'alpha': 0.25, 'ylabel': ylabel, 'clim': clim}, rasterize=True, dynamic=False)

    # Juntamos gráfico de lineas y scatters
    composite_plot = lines_plot * single_channel_scatter_plot * all_channels_scatter_plot * max_line_plot
    
    # del df_with_min_max_avg
    # gc.collect()
    
    #composite_plot = max_line_plot
    return composite_plot.opts(legend_position='top_left', toolbar='above', responsive=True, min_height=400, hooks=[disable_logo])


def update_grid(property_name, panel):  
    match property_name:
        case 'scb_pixel_temperature':
            panels_dict['grid'][0, 0] = panel
        
        case 'scb_temperature':
            panels_dict['grid'][0, 1] = panel
            
        case 'scb_humidity':
            panels_dict['grid'][0, 2] = panel
            
        case 'scb_pixel_an_current':
            panels_dict['grid'][1, 0:2] = panel
        
        case 'scb_pixel_hv_monitored':
            panels_dict['grid'][1, 2:3] = panel
    

def create_plot_panel(initial_data, title, date_picker, property_name, value_field, id_var, var_name, value_name, xlabel, ylabel, remove_temperature_anom, search_previous, cmap, climit):

    @pn.depends(date_picker.param.value, watch=True)
    def update_plot(date_picker):
        with pn.param.set_values(panels_dict[property_name], loading=True):
            print('\n[Updating] ' + property_name +  ' plot')

            # We need to create a new mongodb client connection since this is a fork process and mongodb client is not thread safe 
            db = connect_to_database('localhost', 27017, 'CACO')
            clusco_min_collection = db['CLUSCO_min']

            
            data = get_data_by_date(collection=clusco_min_collection,
                                        property_name=property_name,
                                        date_time=date_picker, value_field=value_field,
                                        id_var=id_var, var_name=var_name,
                                        value_name=value_name, remove_temperature_anom=remove_temperature_anom, search_previous=search_previous)
            
            # close mongodb connection
            db.client.close()
            
            if len(data.index) != 0:
                plot = plot_data(data, id_var, value_name, title, xlabel, ylabel, var_name, cmap, climit)

            else:
                print('No data to plot!')
                plot = empty_plot()
            
            c_widget = pn.widgets.DiscreteSlider
            c_widget.align = 'center'
            c_widget.sizing_mode = 'stretch_width'
            
            plot_panel = pn.panel(plot, widget_location='bottom', widgets={var_name: c_widget}, sizing_mode='stretch_width', linked_axes=False)
            panels_dict.update({property_name: plot_panel})
            
            update_grid(property_name=property_name, panel=plot_panel)
            
            print('[Updating finished]')
            
            # run garbage collector to free memory
            
            del data
            gc.collect()
            
            return plot

    plot = plot_data(initial_data, id_var, value_name,
                              title, xlabel, ylabel, var_name, cmap, climit)

    c_widget = pn.widgets.DiscreteSlider
    c_widget.align = 'center'
    c_widget.sizing_mode = 'stretch_width'

    plot_panel = pn.panel(plot, widget_location='bottom', widgets={var_name: c_widget}, sizing_mode='stretch_width', linked_axes=False)
    panels_dict.update({property_name: plot_panel})
    
    del(initial_data)
    gc.collect()
    
    return plot_panel

def create_dashboard():
    tic = time.perf_counter()
    
    pn.param.ParamMethod.loading_indicator = True

    # Setup BD Connection
    db = connect_to_database('localhost', 27017, 'CACO')
    clusco_min_collection = db['CLUSCO_min']


    date_filter = dt.date.today()
    #date_filter = dt.date(2023, 1, 3) # To test a initial day without data

    # Data retrieved from database
    pacta_temperature_data = get_data_by_date(collection=clusco_min_collection, property_name='scb_pixel_temperature',
                                              date_time=date_filter, value_field='avg', id_var='date', var_name='channel', value_name='temperature', remove_temperature_anom=True)


    # bug uncaught here
    print(pacta_temperature_data)
    # Start date based on the date looked up by pacta_temperature
    # Nos aseguraremos de que los datos correspondan a la misma fecha, en caso de que se haya realizado la consulta en un dia sin datos y se haya buscado en el dia anterior
    if (len(pacta_temperature_data.index) > 0):
        start_date = pacta_temperature_data['date'].dt.date[0]
    else:
        print('Some error was found when retrieving data from database, retrying to recover data')
        while(len(pacta_temperature_data.index) == 0):
            pacta_temperature_data = get_data_by_date(collection=clusco_min_collection, property_name='scb_pixel_temperature',
                                              date_time=date_filter, value_field='avg', id_var='date', var_name='channel', value_name='temperature', remove_temperature_anom=True)
            
    start_date = pacta_temperature_data['date'].dt.date[0]
    # get the first date value from pacta_temperature_data
    
    
    scb_temperature_data = get_data_by_date(collection=clusco_min_collection, property_name='scb_temperature',
                                            date_time=start_date, value_field='avg', id_var='date', var_name='module', value_name='temperature', remove_temperature_anom=True)

    scb_humidity_data = get_data_by_date(collection=clusco_min_collection, property_name='scb_humidity',
                                         date_time=start_date, value_field='avg', id_var='date', var_name='module', value_name='humidity', remove_temperature_anom=False)

    scb_anode_current_data = get_data_by_date(collection=clusco_min_collection, property_name='scb_pixel_an_current',
                                              date_time=start_date, value_field='avg', id_var='date', var_name='channel', value_name='anode', remove_temperature_anom=False)

    hv_data = get_data_by_date(collection=clusco_min_collection, property_name='scb_pixel_hv_monitored',
                               date_time=start_date, value_field='avg', id_var='date', var_name='channel', value_name='hv', remove_temperature_anom=False)

    
    # close mongodb connection
    db.client.close()
    
    date_picker = pn.widgets.DatePicker(
        name='Date Selection', value=start_date, end=dt.date.today())

    print("\nMaking plots...")
    #pacta_temp_plot_panel = manage_pacta_plot(clusco_min_collection, pacta_temperature_data, start_date, date_picker)
    
    # Custom color maps for scatter plots
    cmap_temps = LinearSegmentedColormap.from_list('cmap_temps', [(
        0, (0, 0, 1)), (18/30, (0, 1, 0)), (25/30, (1, 0.65, 0)), (26/30, (1, 0, 0)), (1, (1, 0, 0))])
    
    cmap_humidty = LinearSegmentedColormap.from_list('cmap_humidity', [
        (0, (1, 0.64, 0)),
        (10/80, (1, 0.92, 0)),
        (40/80, (0, 1, 0)),
        (70/80, (1, 0.92, 0)),
        (74/80, (1, 0.64, 0)),
        (80/80, (1, 0, 0))])
    
    cmap_anode = LinearSegmentedColormap.from_list('cmap_anode', [
        (0, (0, 0, 1)),
        (5/100, (0, 1, 0)),
        (60/100, (1, 0.92, 0)),
        (80/100, (1, 0.64, 0)),
        (100/100, (1, 0, 0))])
    
    cmap_hv = LinearSegmentedColormap.from_list('cmap_hv', [
        (0, (0, 0, 1)),
        (10/1400, (0, 0, 1)),
        (200/1400, (0, 1, 0)),
        (950/1400, (1, 0.92, 0)),
        (1200/1400, (1, 0.64, 0)),
        (1400/1400, (1, 0, 0))])
    
    
    pacta_temp_plot_panel = create_plot_panel(pacta_temperature_data, 'PACTA Temperature', date_picker, 'scb_pixel_temperature', 'avg', 'date', 'channel', 'temperature', 'Time', 'Temperature (ºC)', True, False, cmap_temps, (0, 30))
    scb_temp_plot_panel = create_plot_panel(scb_temperature_data, 'SCB Temperature', date_picker, 'scb_temperature', 'avg', 'date', 'module', 'temperature', 'Time', 'Temperature (ºC)', False, False, cmap_temps, (0, 30))
    scb_humidity_plot_panel = create_plot_panel(scb_humidity_data, 'SCB Humidity', date_picker, 'scb_humidity', 'avg', 'date', 'module', 'humidity', 'Time', 'Humidity (%)', False, False, cmap_humidty, (0, 80))
    scb_anode_current_plot_panel = create_plot_panel(scb_anode_current_data, 'Anode Current', date_picker, 'scb_pixel_an_current', 'avg', 'date', 'channel', 'anode', 'Time', 'Anode Current (µA)', False, False, cmap_anode, (0, 100))
    hv_plot_panel = create_plot_panel(hv_data, 'High Voltage', date_picker, 'scb_pixel_hv_monitored', 'avg', 'date', 'channel', 'hv', 'Date', 'HV (V)', False, False, cmap_hv, (10, 1400))
    
    # Creamos grid
    grid = pn.GridSpec(sizing_mode='stretch_both', ncols=3, nrows=2, mode='override')

    # We create two grids in order two group with different columns arrangement
    grid[0, 0] = pacta_temp_plot_panel
    grid[0, 1] = scb_temp_plot_panel
    grid[0, 2] = scb_humidity_plot_panel
    grid[1, 0:2] = scb_anode_current_plot_panel
    grid[1, 2:3] = hv_plot_panel
    
    panels_dict['grid'] =  grid
    
    material_dashboard = pn.template.MaterialTemplate(title='Clusco Reports', header_background='#00204e', favicon='/images/favicon.ico')

    png_pane = pn.pane.PNG('./images/cta-logo.png', width=200, align='center')
    sidebar_col = pn.Column(pn.layout.HSpacer(), png_pane, pn.layout.HSpacer(), date_picker)
    material_dashboard.main.append(grid)
    material_dashboard.sidebar.append(sidebar_col)
    
    #material_dashboard.sidebar.append(date_picker)
    

    toc = time.perf_counter()
    print(f"\nServer started in {toc - tic:0.4f} seconds")
    
    
    del pacta_temperature_data, scb_temperature_data, scb_humidity_data, scb_anode_current_data, hv_data
    gc.collect()
    return material_dashboard
    
    
if __name__ == '__main__':
    pn.serve(create_dashboard(), port=5006, allow_websocket_origin='localhost:5006', websocket_origin='localhost:5006', verbose=True, show=False, static_dirs={'images': './images'}, admin=True, admin_password='admin', dev=True, threaded=True, start=True)

    