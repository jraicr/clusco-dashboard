import pandas as pd
import datetime as dt
import holoviews as hv
import holoviews.operation.datashader as hd
import hvplot.pandas
import panel as pn
import datashader as ds
from pymongo import MongoClient
from matplotlib.colors import LinearSegmentedColormap
import time
import gc
import threading
import sys, os

gc.enable()
#gc.set_debug(gc.DEBUG_STATS | gc.DEBUG_LEAK)

hv.extension('bokeh', logo=False)
pn.extension(loading_spinner='dots', loading_color='#00204e', sizing_mode="stretch_width")
pd.options.plotting.backend = 'holoviews'
pn.config.throttled = True

def restart_server_if_empty_task(interval_sec=60):
    while True:
        time.sleep(interval_sec)
        
        print('Checking if server is empty to perform restart...')
        if pn.state.session_info['live'] == 0:
            print("Restarting server...")
            os.execv(sys.executable, ['python'] + sys.argv)
        
# Global variable to store the thread that will restart the server if it is empty
restart_server_thread_task = threading.Thread(target=restart_server_if_empty_task)
restart_server_thread_task.daemon = True

def disable_logo(plot, element):
    """
    Disables blokeh logo in plots
    """
    plot.state.toolbar.logo = None


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


def hvplot_df_line(df:pd.DataFrame, x, y, title:str, dic_opts:dict, groupby:str, color:str='green'):
    """
    Plots a pandas dataframe line plot.
    """
    
    dynamic_map = df.hvplot.line(x=x, y=y, title=title, color=color, groupby=groupby,
                                 label='selected ' + groupby,  responsive=True, min_height=400, muted_alpha=0)

    
    options = list(dic_opts.items())
    dynamic_map.opts(**dict(options))

    return dynamic_map


def hvplot_df_max_min_avg_line(df, x, y, title, dic_opts, category):
    """_summary_
    Plots a pandas dataframe line plot with max, min and average columns in df.
    # TODO: Refactorize this to use only one function for all line plots.
    """

    # dynamic_map = df.hvplot.line(x=x, y=['max', 'min', 'mean'], title=title,
    #                              responsive=True, min_height=400, hover_cols=[x,y, 'max_' + category, 'min_' + category])
    max_line = df.hvplot.line(x=x, y='max', title=title, responsive=True, min_height=400, hover_cols=['x', 'y', 'max_' + category], label='max', color='red').opts(show_legend=True, alpha=1, muted_alpha=0)
        
    min_line = df.hvplot.line(x=x, y='min', title=title, responsive=True, min_height=400, hover_cols=['x', 'y', 'min_' + category], label='min', color='blue').opts(show_legend=True, alpha=1, muted_alpha=0)
    
    mean_line = df.hvplot.line(x=x, y='avg', title=title, responsive=True, min_height=400, hover_cols=['x', 'y'], label='avg', color='black').opts(show_legend=True, alpha=1, muted_alpha=0)
    
    options = list(dic_opts.items())
    
    dynamic_map = max_line * mean_line * min_line
    #dynamic_map.opts(**dict(options))

    return dynamic_map


def hvplot_df_scatter(df, x, y, title, color, size, marker, dic_opts, cmap="reds", groupby=None, datashade=False, rasterize=False, dynamic=True):
    """_summary_
    Plot a scatter graph from a pandas dataframe
    """
    if rasterize == False:
        plot = df.hvplot.scatter(x=x, y=y, title=title, color=color, label='selected ' + groupby,
                                 size=size, marker=marker, cmap=cmap, groupby=groupby, datashade=datashade, rasterize=rasterize, dynamic=dynamic, responsive=True, min_height=400)

    else:
        plot = df.hvplot.scatter(x=x, y=y, title=title, color=color,
                                 marker=marker, cmap=cmap, groupby=groupby, datashade=datashade, rasterize=rasterize, dynamic=dynamic, responsive=True, min_height=400, label='mean rasterized values')

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
        # print('Building pandas dataframe...')

        # Pandas dataframe
        pandas_df = pd.DataFrame(data_values, columns=[
            var_name+f"_{i+1}" for i in range(len(data_values[0]))])

        # Add dates to dataframe and sort by date
        pandas_df['date'] = pd.to_datetime(datetime_values)

        # Melt dataframe to converts from width df to long,
        # where channel would be a variable and the temperature the value...
        #print('Transforms pandas dataframe from wide to long...')
        pandas_df = pandas_df.melt(
            id_vars=[id_var], var_name=var_name, value_name=value_name)

        if remove_temperature_anom:
            # Removes 0 values
            values_field_name_attr = getattr(pandas_df, value_name)
            pandas_df = pandas_df.loc[values_field_name_attr != 0]

            # Remove all values below -25
            pandas_df = pandas_df.loc[values_field_name_attr > -25]

            # Remove all values above 250
            pandas_df = pandas_df.loc[values_field_name_attr < 250]

        # Removes 'channel_' from channel rows values
        pandas_df[var_name] = pandas_df[var_name].str.replace(
            var_name+'_', '')

        # convert var name type to int
        pandas_df[var_name] = pandas_df[var_name].astype('uint16', copy=False)
        pandas_df.set_index('date', inplace=True)

        # sort by date
        pandas_df.sort_index(inplace=True)

        # Print pandas dataframe memory usage to console
        #pandas_df.info(memory_usage='deep')

        del data_values, datetime_values
        gc.collect()

    else:  # Return a empty pandas dataframe in case no data is found
        pandas_df = pd.DataFrame()

    return pandas_df


def empty_plot():
    # Creates and return an empty plot
    empty_plot = hv.Curve([])

    # Add text the plot indicating "There is no available data in the selected date"
    empty_plot = empty_plot * \
        hv.Text(0.6, 0.6, 'There is no available data in the selected date')

    return empty_plot


def build_min_max_avg(df, x, y, category):
    """_summary_
    Build a dataframe with the min, max and avg values for each date
    """
    def process_chunk(df_chunk):
        # Group by date and get the max, min and avg values
        df_agg = df_chunk.groupby(x).agg(
            max=(y, 'max'),
            min=(y, 'min'),
            mean=(y, 'mean'),
        ).rename(columns={'mean':'avg'}).reset_index()

        # Merge with the original DataFrame to get the category for the max and min values
        df_agg = df_agg.merge(
            df_chunk,
            left_on=[x, 'max'],
            right_on=[x, y],
            how='left'
        ).rename(columns={category: 'max_' + category}).drop(columns=y)

        df_agg = df_agg.merge(
            df_chunk,
            left_on=[x, 'min'],
            right_on=[x, y],
            how='left'
        ).rename(columns={category: 'min_' + category}).drop(columns=y)

        # Set the date column as the index
        df_agg.set_index(x, inplace=True)

        def join_unique_values(series):
            return ','.join(set(series.astype(str)))

        # Group by date and concatenate unique categories with commas
        df_agg = df_agg.groupby(x).agg({'max': 'first', 'min': 'first', 'avg': 'first', 'max_' + category: join_unique_values, 'min_' + category: join_unique_values})

        return df_agg

    # Split the data into chunks and process each chunk separately
    chunksize = 40000
    result = []
    for i in range(0, len(df), chunksize):
        df_chunk = df.iloc[i:i+chunksize]
        result.append(process_chunk(df_chunk))

    # Concatenate the results
    df_agg = pd.concat(result)

    return df_agg

def build_dataframe_with_min_max(df, x, y):
   # Build a pandas dataframe from the original dataframe and select the min and max values for each datetime value  
   
    new_df = df.groupby(x)[y].agg(['min', 'max', 'mean'])
    new_df = new_df.reset_index()
    #new_df = new_df.rename(columns={'min': 'min_'+y, 'max': 'max_'+y, 'mean': 'avg_'+y})
    new_df.rename(columns={'mean': 'avg'}, inplace=True)

    # index by date
    new_df.set_index(x, inplace=True)

    # Sort by date
    new_df.sort_index(inplace=True)

    return new_df


def plot_data(data, x, y, title, xlabel, ylabel, groupby, cmap_custom, clim):

    # Build a pandas dataframe from the original dataframe and select the min and max values for each date corresponding to each group
    # df_with_min_max_avg = build_dataframe_with_min_max(data, x, y)
    df_with_min_max_avg = build_min_max_avg(data, x, y, groupby)

    max_line_plot = hvplot_df_max_min_avg_line(df_with_min_max_avg, x=x, y=y, title=title, dic_opts={
        'padding': 0.1, 'tools': ['hover'], 'xlabel': xlabel, 'ylabel': ylabel, 'axiswise': True, 'show_legend': True}, category=groupby)

    # Plot lines grouped by channel from data (channel is selected by widget and just one channel is shown at a time)
    lines_plot = hvplot_df_line(data, x=x, y=y, title=title, dic_opts={
        'padding': 0.1, 'tools': ['hover'], 'xlabel': xlabel, 'ylabel': ylabel, 'axiswise': True}, groupby=groupby, color='purple')

    # Plot scatter from data for a single channel
    single_channel_scatter_plot = hvplot_df_scatter(data, x=x, y=y, title=title, color=y, cmap=cmap_custom, size=15, marker='o', dic_opts={
        'padding': 0.1, 'tools': [''], 'xlabel': xlabel, 'ylabel': ylabel, 'clim': clim, 'alpha': 0.5}, groupby=groupby)

    # Plot scatter from data for all channels (rasterized)
    all_channels_scatter_plot = hvplot_df_scatter(data, x=x, y=y, title=title, color=y, cmap=cmap_custom,  size=20, marker='o', dic_opts={
                                                  'padding': 0.1, 'tools': [''], 'xlabel': xlabel, 'alpha': 0.15, 'ylabel': ylabel, 'clim': clim}, rasterize=True, dynamic=False)

    # Juntamos gráfico de lineas y scatters
    composite_plot = lines_plot * single_channel_scatter_plot  * max_line_plot * all_channels_scatter_plot

    # composite_plot = lines_plot * single_channel_scatter_plot * \
    #     all_channels_scatter_plot
        
    # Not sure if this have any effects since we are using hvplot_df_max_min_avg_line with this dataframe
    del df_with_min_max_avg, data
    gc.collect()

    return composite_plot.opts(legend_position='top', toolbar='above', responsive=True, min_height=500, hooks=[disable_logo], show_legend=True, legend_opts={"click_policy": "hide"})

def create_plot_panel(initial_data, title, date_picker, property_name, value_field, id_var, var_name, value_name, xlabel, ylabel, remove_temperature_anom, search_previous, cmap, climit, template):

    template.main[0][0][2].object = f'''<h1 style="text-align:center">Making plots...</h1> <h2 style="text-align:center">({title})</h2> '''
    print("   - Creating plot panel for: " + title)
    
    @pn.depends(date_picker.param.value, watch=True)
    def thread_update_plot_task(date_picker):

        t = threading.Thread(target=update_plot, args=(date_picker, ))
        t.daemon = False
        t.start()

    def get_panel_by_property(panel_name_id):
        result_panel = None
        match panel_name_id:
            case 'scb_pixel_temperature':
                result_panel = template.main[0][0][0, 0]

            case 'scb_temperature':
                result_panel = template.main[0][0][0, 1]

            case 'scb_humidity':
                result_panel = template.main[0][0][0, 2]

            case 'scb_pixel_an_current':
                result_panel = template.main[0][0][1, 0]

            case 'scb_pixel_hv_monitored':
                result_panel = template.main[0][0][1, 2]

        print('result_panel in getter', result_panel)
        return result_panel

    def update_plot(date_picker):
        with pn.param.set_values(get_panel_by_property(property_name), loading=True):

            print('\n[Updating] ' + property_name + ' plot')

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
                updated_plot = plot_data(data, id_var, value_name, title + ' (' + str(
                    date_picker) + ')', xlabel, ylabel, var_name, cmap, climit)

            else:
                print('No data to plot!')
                updated_plot = empty_plot()

            c_widget = pn.widgets.DiscreteSlider
            c_widget.align = 'center'
            c_widget.sizing_mode = 'stretch_width'

            updated_plot_panel = pn.panel(updated_plot, widget_location='bottom', widgets={
                                          var_name: c_widget}, sizing_mode='stretch_width', linked_axes=False)

            update_grid(panel=updated_plot_panel)
            print(f'[Update Finished] {title}')

            # run garbage collector to free memory
            del data, db, clusco_min_collection
            gc.collect()

            return plot

    @pn.io.with_lock
    def update_grid(panel):

        match property_name:
            case 'scb_pixel_temperature':
                template.main[0][0][0, 0] = panel

            case 'scb_temperature':
                template.main[0][0][0, 1] = panel

            case 'scb_humidity':
                template.main[0][0][0, 2] = panel

            case 'scb_pixel_an_current':
                template.main[0][0][1, 0:2] = panel

            case 'scb_pixel_hv_monitored':
                template.main[0][0][1, 2:3] = panel

        gc.collect()

    plot = plot_data(initial_data, id_var, value_name,
                     title + ' (' + str(date_picker.value) + ')', xlabel, ylabel, var_name, cmap, climit)

    c_widget = pn.widgets.DiscreteSlider
    c_widget.align = 'center'
    c_widget.sizing_mode = 'stretch_width'

    plot_panel = pn.panel(plot, widget_location='bottom', widgets={
                          var_name: c_widget}, sizing_mode='stretch_width', linked_axes=False)

    del initial_data
    gc.collect()

    return plot_panel


def create_dashboard(template):

    print('Creating dashboard')
    
    tic = time.perf_counter()

    pn.param.ParamMethod.loading_indicator = True

    template.main[0][0][2].object = '''<h1 style="text-align:center">Getting data...</h1>'''

    # Setup BD Connection
    db = connect_to_database('localhost', 27017, 'CACO')

    if (db == None):
        template.main[0][0] = pn.Column()
        template.sidebar[0][0] = pn.Column()

        db_error_image = pn.pane.PNG(
            './images/db_error.png', width=100, align='center')
        
        db_error_text = pn.pane.Markdown('''<h1 style="text-align:center">Connection to database failed.
            Check if the database is running.</h1>''')
        
        main_error_col = pn.Column(pn.layout.VSpacer(
        ), db_error_image,  db_error_text, pn.layout.VSpacer(), sizing_mode='stretch_both')

        template.main[0][0] = main_error_col

        exit()

    clusco_min_collection = db['CLUSCO_min']

    date_filter = dt.date.today()
    #date_filter = dt.date(2023, 1, 3)  # This is to make a test to run the app with a initial day without data

    # Data retrieved from database
    pacta_temperature_data = get_data_by_date(collection=clusco_min_collection, property_name='scb_pixel_temperature',
                                              date_time=date_filter, value_field='avg', id_var='date', var_name='channel', value_name='temperature', remove_temperature_anom=False)

    # There is a bug very hard to reproduces at this point, it happens sometimes when trying to start the server, but retrying it works...
    # print(pacta_temperature_data)
    # Start date based on the date looked up by pacta_temperature
    if (len(pacta_temperature_data.index) > 0):
        #start_date = pacta_temperature_data['date'].dt.date[0]
        start_date = pacta_temperature_data.index[0].date()
    else:
        print('No data recovered...')
        sys.exit()
        # while(len(pacta_temperature_data.index) == 0):
        #     pacta_temperature_data = get_data_by_date(collection=clusco_min_collection, property_name='scb_pixel_temperature',
        #                                               date_time=date_filter, value_field='avg', id_var='date', var_name='channel', value_name='temperature', remove_temperature_anom=True)

    # get the first date value from pacta_temperature_data
    # the index dataframe is the date, so we can get the first date value from pacta_temperature_data
    start_date = pacta_temperature_data.index[0].date()

    scb_temperature_data = get_data_by_date(collection=clusco_min_collection, property_name='scb_temperature',
                                            date_time=start_date, value_field='avg', id_var='date', var_name='module', value_name='temperature', remove_temperature_anom=False)

    scb_humidity_data = get_data_by_date(collection=clusco_min_collection, property_name='scb_humidity',
                                         date_time=start_date, value_field='avg', id_var='date', var_name='module', value_name='humidity', remove_temperature_anom=False)

    scb_anode_current_data = get_data_by_date(collection=clusco_min_collection, property_name='scb_pixel_an_current',
                                              date_time=start_date, value_field='avg', id_var='date', var_name='channel', value_name='anode', remove_temperature_anom=False)

    hv_data = get_data_by_date(collection=clusco_min_collection, property_name='scb_pixel_hv_monitored',
                               date_time=start_date, value_field='avg', id_var='date', var_name='channel', value_name='hv', remove_temperature_anom=False)

    # close mongodb connection
    db.client.close()

    template.main[0][0][2].object = '''<h1 style="text-align:center">Making plots...</h1>'''

    date_picker = pn.widgets.DatePicker(
        name='Date Selection', value=start_date, end=dt.date.today())

    print("\nMaking plots...")

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

    pacta_temp_plot_panel = create_plot_panel(pacta_temperature_data, 'PACTA Temperature', date_picker, 'scb_pixel_temperature',
                                              'avg', 'date', 'channel', 'temperature', 'Time (UTC)', 'Temperature (ºC)', True, False, cmap_temps, (0, 30), template)
    scb_temp_plot_panel = create_plot_panel(scb_temperature_data, 'SCB Temperature', date_picker, 'scb_temperature',
                                            'avg', 'date', 'module', 'temperature', 'Time (UTC)', 'Temperature (ºC)', False, False, cmap_temps, (0, 30), template)
    scb_humidity_plot_panel = create_plot_panel(scb_humidity_data, 'SCB Humidity', date_picker, 'scb_humidity',
                                                'avg', 'date', 'module', 'humidity', 'Time (UTC)', 'Humidity (%)', False, False, cmap_humidty, (0, 80), template)
    scb_anode_current_plot_panel = create_plot_panel(scb_anode_current_data, 'Anode Current', date_picker, 'scb_pixel_an_current',
                                                     'avg', 'date', 'channel', 'anode', 'Time (UTC)', 'Anode Current (µA)', False, False, cmap_anode, (0, 100), template)
    hv_plot_panel = create_plot_panel(hv_data, 'High Voltage', date_picker, 'scb_pixel_hv_monitored',
                                      'avg', 'date', 'channel', 'hv', 'Date', 'HV (V)', False, False, cmap_hv, (10, 1400), template)

    template.main[0][0][2].object = '''<h1 style="text-align:center">Deploying dashboard...</h1>'''

    # Creates a grid from GridSpec and adds plots to it
    grid = pn.GridSpec(sizing_mode='stretch_both',
                       ncols=3, nrows=2, mode='override')

    grid[0, 0] = pacta_temp_plot_panel
    grid[0, 1] = scb_temp_plot_panel
    grid[0, 2] = scb_humidity_plot_panel
    grid[1, 0:2] = scb_anode_current_plot_panel
    grid[1, 2:3] = hv_plot_panel

    #print ("Grid Object Overview", grid.objects)

    template.sidebar[0][0][2].object = '''<h1 style="text-align:center">Generating widgets...</h1>'''
    png_pane = pn.pane.PNG('./images/cta-logo.png', width=200, align='center')
    sidebar_col = pn.Column(pn.layout.HSpacer(), png_pane,
                            pn.layout.HSpacer(), date_picker)

    # Append grid to template main
    #print("Updating", template.main.objects)
    template.main[0].sizing_mode = 'stretch_both'
    template.main[0][0] = grid

    # Append content to template sidebar
    #print("Updating", template.main.objects)
    template.sidebar.objects[0].sizing_mode = 'stretch_both'
    template.sidebar[0][0] = sidebar_col

    toc = time.perf_counter()
    print(f"\nServer started in {toc - tic:0.4f} seconds")

    del pacta_temperature_data, scb_temperature_data, scb_humidity_data, scb_anode_current_data, hv_data, clusco_min_collection, db, pacta_temp_plot_panel, scb_temp_plot_panel, scb_humidity_plot_panel, scb_anode_current_plot_panel, hv_plot_panel
    gc.collect()
    
def destroyed(session_context):
    print("Session destroyed", session_context)
    gc.collect()
    
    if  not restart_server_thread_task.is_alive():
        print("Starting restart server thread...")
        restart_server_thread_task.start()



    
def get_user_page():

    material_dashboard = pn.template.MaterialTemplate(
        title='Clusco Reports', header_background='#00204e', favicon='/images/favicon.ico')

    # MAIN
    loading = pn.indicators.LoadingSpinner(
        value=True, width=100, height=100, align='center')

    loading_text = pn.pane.Markdown(
        '''<h1 style="text-align:center">Loading... </h1>''')

    material_dashboard.main.append(pn.Column(pn.Column(pn.layout.VSpacer(
    ), loading, loading_text, pn.layout.VSpacer(), sizing_mode='stretch_both')))

    # SIDEBAR
    loading_sidebar = pn.indicators.LoadingSpinner(value=True, width=25, height=25, align='center')

    logo_sidebar = pn.pane.PNG('./images/cta-logo.png', width=200, align='center')
    loading_text_sidebar = pn.pane.Markdown(''' ''')

    material_dashboard.sidebar.append(pn.Column(pn.Column(pn.layout.VSpacer(), logo_sidebar, pn.layout.VSpacer(), loading_sidebar, loading_text_sidebar, pn.layout.VSpacer(), sizing_mode='stretch_both')))

    # Config callback when session is destroyed
    pn.state.on_session_destroyed(destroyed)

    create_dashboard_thread_task = threading.Thread(target=create_dashboard, args=(material_dashboard, ))
    create_dashboard_thread_task.daemon = True
    create_dashboard_thread_task.start()
    
    return material_dashboard


if __name__ == '__main__':
    pn.serve(get_user_page, port=5006, show=False, static_dirs={'images': './images'}, admin=True, title='Clusco Reports',
             threaded=True, n_threads=4, check_unused_sessions_milliseconds=5000, unused_session_lifetime=5000, log_level='debug')
