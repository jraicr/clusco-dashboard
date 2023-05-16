# Description: This file contains the main application code for the CLUSCO dashboard

import pandas as pd
import datetime as dt
import holoviews as hv # noqa
import holoviews.operation.datashader as hd # noqa
import hvplot.pandas # noqa
import panel as pn
import datashader as ds # noqa
from matplotlib.colors import LinearSegmentedColormap
import time
import gc
import threading
import sys
import os

# Application modules
import database
import dashboard_utils
import plot_helper


gc.enable()
#gc.set_debug(gc.DEBUG_STATS | gc.DEBUG_LEAK)

# Holoviz and Pandas Settings
hv.extension('bokeh', logo=False) 
pn.extension(loading_spinner='dots', loading_color='#00204e', sizing_mode="stretch_width")
pd.options.plotting.backend = 'holoviews'
pn.config.throttled = True


# Custom color maps recreated from this bars: https://camera.lst1.iac.es/mon0
cmap_temps = LinearSegmentedColormap.from_list('cmap_temps', [
    (0, (0, 0, 1)),
    (18/30, (0, 1, 0)),
    (25/30, (1, 0.65, 0)),
    (26/30, (1, 0, 0)),
    (1, (1, 0, 0))])

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

cmap_backplane_temp = LinearSegmentedColormap.from_list('cmap_bp_temp', [
    (0, (0, 0, 1)),
    (20/37, (0, 1, 0)),
    (30/37, (1, 0.99, 0.22)),
    (33/37, (1, 0.64, 0)),
    (35/37, (1, 0, 0)),
    (37/37, (1, 0, 0))])

def restart_server_if_empty_task(interval_sec=60):
    # This function will restart the server if it is empty
    while True:
        time.sleep(interval_sec)
        
        print('Checking if server is empty to perform restart...')
        if pn.state.session_info['live'] == 0:
            print("Restarting server...")
            os.execv(sys.executable, ['python'] + sys.argv)
        
# Global variable to store the thread that will restart the server if it is empty
restart_server_thread_task = threading.Thread(target=restart_server_if_empty_task)
restart_server_thread_task.daemon = True

def create_plot_panel(df, title, date_picker, id_var, var_name, value_name, xlabel, ylabel, cmap, climit, template, show_loading_msg=True):

    if show_loading_msg:
        dashboard_utils.update_loading_message(template, f'''<h1 style="text-align:center">Making plots...</h1> <h2 style="text-align:center">({title})</h2> ''')

    print("   - Creating plot panel for: " + title)

    if isinstance(date_picker, pn.widgets.input.DatePicker):
        date_filter = date_picker.value
    else:
        date_filter = date_picker

    if (df.empty):
        print("   - No data to plot for: " + title)
        plot = plot_helper.create_empty_plot()
        plot_panel = pn.panel(plot, sizing_mode='stretch_width', linked_axes=False)
    
    else:
        plot = plot_helper.plot_data(df, id_var, value_name,
                        title + ' (' + str(date_filter) + ')', xlabel, ylabel, var_name, cmap, climit)

        c_widget = pn.widgets.DiscreteSlider
        c_widget.align = 'center'
        c_widget.sizing_mode = 'stretch_width'

        plot_panel = pn.panel(plot, widget_location='bottom', widgets={
                            var_name: c_widget}, sizing_mode='stretch_width', linked_axes=False)


    return plot_panel


def create_l1_rate_plot_panel(dataList, title, date_picker, id_var, var_name, value_name, xlabel, ylabel, cmap, climit, template, show_loading_msg=True):

    df = dataList[0]

    if show_loading_msg:
        dashboard_utils.update_loading_message(template, f'''<h1 style="text-align:center">Making plots...</h1> <h2 style="text-align:center">({title})</h2> ''')

    print("   - Creating plot panel for: " + title)

    if isinstance(date_picker, pn.widgets.input.DatePicker):
        date_filter = date_picker.value
    else:
        date_filter = date_picker

    if (df.empty):
        print("   - No data to plot for: " + title)
        plot = plot_helper.create_empty_plot()
        plot_panel = pn.panel(plot, sizing_mode='stretch_width', linked_axes=False)
    
    else:
        plot = plot_helper.plot_l1_rate_data(dataList, id_var, value_name,
                        title + ' (' + str(date_filter) + ')', xlabel, ylabel, var_name, cmap, climit)

        c_widget = pn.widgets.DiscreteSlider
        c_widget.align = 'center'
        c_widget.sizing_mode = 'stretch_width'

        plot_panel = pn.panel(plot, widget_location='bottom', widgets={
                            var_name: c_widget}, sizing_mode='stretch_width', linked_axes=False)


    return plot_panel

def update_dashboard(template, date_picker):
    print('Updating dashboard')
        
    tic = time.perf_counter()

    pn.param.ParamMethod.loading_indicator = True

    # Setup BD Connection
    db = database.connect('localhost', 27017, 'CACO')

    if (db == None):
        # dashboard_utils.display_database_error(template=template, show_alert=True)
        dashboard_utils.display_database_error(template=template)
        exit()

    clusco_min_collection = db['CLUSCO_min']

     # Data retrieved from database
    pacta_temperature_data = database.get_data_by_date(collection=clusco_min_collection, property_name='scb_pixel_temperature',
                                            date_time=date_picker, value_field='avg', id_var='date', var_name='channel', value_name='temperature', search_previous=False)


    scb_temperature_data = database.get_data_by_date(collection=clusco_min_collection, property_name='scb_temperature',
                                            date_time=date_picker, value_field='avg', id_var='date', var_name='module', value_name='temperature', search_previous=False)

    scb_humidity_data = database.get_data_by_date(collection=clusco_min_collection, property_name='scb_humidity',
                                        date_time=date_picker, value_field='avg', id_var='date', var_name='module', value_name='humidity', search_previous=False)

    scb_anode_current_data = database.get_data_by_date(collection=clusco_min_collection, property_name='scb_pixel_an_current',
                                            date_time=date_picker, value_field='avg', id_var='date', var_name='channel', value_name='anode', search_previous=False)

    hv_data = database.get_data_by_date(collection=clusco_min_collection, property_name='scb_pixel_hv_monitored',
                            date_time=date_picker, value_field='avg', id_var='date', var_name='channel', value_name='hv', search_previous=False)
    
    scb_backplane_temperature_data = database.get_data_by_date(collection=clusco_min_collection, property_name='backplane_temperature',
                                            date_time=date_picker, value_field='avg', id_var='date', var_name='module', value_name='temperature', search_previous=False)


    l1_rate_data = database.get_data_by_date(collection=clusco_min_collection, property_name='l1_rate',
                                            date_time=date_picker, value_field='avg', id_var='date', var_name='module', value_name='l1_rate', search_previous=False)
    
    l1_rate_control_data = database.get_scalar_data_by_date(collection=clusco_min_collection, property_name='clusco_l1_rate_control',
                                            date_time=date_picker, value_field='avg', id_var='date', var_name='module', value_name='l1_rate_control', remove_zero_values=True, search_previous=False)
    
    l0_rate_control_data = database.get_scalar_data_by_date(collection=clusco_min_collection, property_name='clusco_l0_rate_control',
                                            date_time=date_picker, value_field='avg', id_var='date', var_name='module', value_name='l0_rate_control', remove_zero_values=True, search_previous=False)


    # close mongodb connection
    db.client.close()

    print("\nMaking plots...")

    # Panels for the first dashboard tab
    pacta_temp_plot_panel = create_plot_panel(pacta_temperature_data, 'PACTA Temperature', date_picker, 'date', 'channel', 'temperature', 'Time (UTC)', 'Temperature (ºC)', cmap_temps, (0, 30), template, False)
    template.main[0][0][0][0, 0] = pacta_temp_plot_panel
    
    scb_temp_plot_panel = create_plot_panel(scb_temperature_data, 'SCB Temperature', date_picker, 'date', 'module', 'temperature', 'Time (UTC)', 'Temperature (ºC)', cmap_temps, (0, 30), template, False)
    template.main[0][0][0][0, 1] = scb_temp_plot_panel
    
    scb_humidity_plot_panel = create_plot_panel(scb_humidity_data, 'SCB Humidity', date_picker, 'date', 'module', 'humidity', 'Time (UTC)', 'Humidity (%)', cmap_humidty, (0, 80), template, False)
    template.main[0][0][0][0, 2] = scb_humidity_plot_panel

    scb_anode_current_plot_panel = create_plot_panel(scb_anode_current_data, 'Anode Current', date_picker, 'date', 'channel', 'anode', 'Time (UTC)', 'Anode Current (µA)', cmap_anode, (0, 100), template, False)
    template.main[0][0][0][1, 0] = scb_anode_current_plot_panel

    hv_plot_panel = create_plot_panel(hv_data, 'High Voltage', date_picker, 'date', 'channel', 'hv', 'Date', 'HV (V)', cmap_hv, (10, 1400), template, False)
    template.main[0][0][0][1, 1] = hv_plot_panel

    scb_backplane_temp_plot_panel = create_plot_panel(scb_backplane_temperature_data, 'SCB Backplane Temperature', date_picker, 'date', 'module', 'temperature', 'Time (UTC)', 'Temperature (ºC)', cmap_backplane_temp, (0, 37), template, False)
    template.main[0][0][0][1, 2] = scb_backplane_temp_plot_panel

    # Panels for the second dashboard tab
    l1_rate_plot_panel = create_l1_rate_plot_panel([l1_rate_data, l0_rate_control_data, l1_rate_control_data], 'L1 Rate', date_picker, 'date', 'module', 'l1_rate', 'Time (UTC)', 'L1 Rate (Hz)', cmap_temps, (0, 1000), template, False)

    template.main[0][0][1][0, 0] = l1_rate_plot_panel

    toc = time.perf_counter()
    print(f"\Dashboard updated in {toc - tic:0.4f} seconds")


def create_dashboard(template, date_filter=dt.date.today()):

    print('Creating dashboard')
        
    tic = time.perf_counter()

    pn.param.ParamMethod.loading_indicator = True

    dashboard_utils.update_loading_message(template, '''<h1 style="text-align:center">Getting data...</h1>''')

    # Setup BD Connection
    db = database.connect('localhost', 27017, 'CACO')

    if (db == None):
        dashboard_utils.display_database_error(template=template)
        exit()

    clusco_min_collection = db['CLUSCO_min']

    # Data retrieved from database
    pacta_temperature_data = database.get_data_by_date(collection=clusco_min_collection, property_name='scb_pixel_temperature',
                                            date_time=date_filter, value_field='avg', id_var='date', var_name='channel', value_name='temperature')

    
    # Start date based on the date looked up by pacta_temperature
    if (len(pacta_temperature_data.index) > 0):
        # get the first date value from pacta_temperature_data
        # the index dataframe is the date, so we can get the first date value from pacta_temperature_data
        start_date = pacta_temperature_data.index[0].date()
    else:
        print('No data recovered...')
        sys.exit()
    
    scb_temperature_data = database.get_data_by_date(collection=clusco_min_collection, property_name='scb_temperature',
                                            date_time=start_date, value_field='avg', id_var='date', var_name='module', value_name='temperature')

    scb_humidity_data = database.get_data_by_date(collection=clusco_min_collection, property_name='scb_humidity',
                                        date_time=start_date, value_field='avg', id_var='date', var_name='module', value_name='humidity')

    scb_anode_current_data = database.get_data_by_date(collection=clusco_min_collection, property_name='scb_pixel_an_current',
                                            date_time=start_date, value_field='avg', id_var='date', var_name='channel', value_name='anode')

    hv_data = database.get_data_by_date(collection=clusco_min_collection, property_name='scb_pixel_hv_monitored',
                            date_time=start_date, value_field='avg', id_var='date', var_name='channel', value_name='hv')
    
    scb_backplane_temperature_data = database.get_data_by_date(collection=clusco_min_collection, property_name='backplane_temperature',
                                            date_time=start_date, value_field='avg', id_var='date', var_name='module', value_name='temperature')


    l1_rate_data = database.get_data_by_date(collection=clusco_min_collection, property_name='l1_rate',
                                            date_time=start_date, value_field='avg', id_var='date', var_name='module', value_name='l1_rate')
    
    
    l1_rate_control_data = database.get_scalar_data_by_date(collection=clusco_min_collection, property_name='clusco_l1_rate_control',
                                            date_time=start_date, value_field='avg', id_var='date', var_name='module', value_name='l1_rate_control', remove_zero_values=True)
    

    l0_rate_control_data = database.get_scalar_data_by_date(collection=clusco_min_collection, property_name='clusco_l0_rate_control',
                                            date_time=start_date, value_field='avg', id_var='date', var_name='module', value_name='l0_rate_control', remove_zero_values=True)
    
    l1_rate_max_data = database.get_scalar_data_by_date(collection=clusco_min_collection, property_name='clusco_l1_rate_max',
                                            date_time=start_date, value_field='avg', id_var='date', var_name='module', value_name='l1_rate_max')

    print(l1_rate_max_data)
    # close mongodb connection
    db.client.close()

    dashboard_utils.update_loading_message(template, '''<h1 style="text-align:center">Making plots...</h1>''')

    date_picker = pn.widgets.DatePicker(
        name='Date Selection', value=start_date, end=dt.date.today())

    print("\nMaking plots...")

    # First dashboard tab
    pacta_temp_plot_panel =  create_plot_panel(pacta_temperature_data, 'PACTA Temperature', date_picker, 'date', 'channel', 'temperature', 'Time (UTC)', 'Temperature (ºC)', cmap_temps, (0, 30), template) 
    scb_temp_plot_panel = create_plot_panel(scb_temperature_data, 'SCB Temperature', date_picker, 'date', 'module', 'temperature', 'Time (UTC)', 'Temperature (ºC)', cmap_temps, (0, 30), template)
    scb_humidity_plot_panel = create_plot_panel(scb_humidity_data, 'SCB Humidity', date_picker, 'date', 'module', 'humidity', 'Time (UTC)', 'Humidity (%)', cmap_humidty, (0, 80), template)
    scb_anode_current_plot_panel = create_plot_panel(scb_anode_current_data, 'Anode Current', date_picker, 'date', 'channel', 'anode', 'Time (UTC)', 'Anode Current (µA)', cmap_anode, (0, 100), template)
    hv_plot_panel = create_plot_panel(hv_data, 'High Voltage', date_picker, 'date', 'channel', 'hv', 'Date', 'HV (V)', cmap_hv, (10, 1400), template)
    scb_backplane_temp_plot_panel = create_plot_panel(scb_backplane_temperature_data, 'SCB Backplane Temperature', date_picker, 'date', 'module', 'temperature', 'Time (UTC)', 'Temperature (ºC)', cmap_backplane_temp, (0, 37), template)

    # Second dashboard tab
    l1_rate_plot_panel = create_l1_rate_plot_panel([l1_rate_data, l0_rate_control_data, l1_rate_control_data, l1_rate_max_data], 'L1 Rate', date_picker, 'date', 'module', 'l1_rate', 'Time (UTC)', 'L1 Rate (Hz)', cmap_temps, (0, 1000), template)

    dashboard_utils.update_loading_message(template, '''<h1 style="text-align:center">Deploying dashboard...</h1>''')

    # =========================
    # FIRST GRID - FIRST TAB
    # =========================
    # Creates a grid from GridSpec and adds plots to it
    grid = pn.GridSpec(sizing_mode='stretch_both',
                    ncols=3, nrows=2, mode='override')

    grid[0, 0] = pacta_temp_plot_panel
    grid[0, 1] = scb_temp_plot_panel
    grid[0, 2] = scb_humidity_plot_panel
    grid[1, 0] = scb_anode_current_plot_panel
    grid[1, 1] = hv_plot_panel
    grid[1, 2] = scb_backplane_temp_plot_panel

    # =========================
    # SECOND GRID - SECOND TAB
    # =========================
    grid_b = pn.GridSpec(sizing_mode='stretch_width',
                       
                    ncols=3, nrows=2, mode='override')
    grid_b[0, 0] = l1_rate_plot_panel

    png_pane = pn.pane.PNG('./images/cta-logo.png', width=200, align='center')
    sidebar_col = pn.Column(pn.layout.HSpacer(), png_pane,
                            pn.layout.HSpacer(), date_picker)

    # Append tabs and grids to template main
    template.main[0].sizing_mode = 'stretch_both'
    
    # Creating tabs and appending grids to it
    tabs = pn.Tabs(('Pixel Temp, Anode and HV - SCB Temp and Humidity', grid), ('Rates', grid_b))

    # template.main[0][0] = grid
    template.main[0][0] = tabs

    # Append content to template sidebar
    template.sidebar.objects[0].sizing_mode = 'stretch_both'
    template.sidebar[0][0] = sidebar_col

    toc = time.perf_counter()
    print(f"\nServer started in {toc - tic:0.4f} seconds")

    @pn.depends(date_picker.param.value, watch=True)
    def thread_update_dashboard_task(date_picker):

        # Iterates each tab to activates loading indicator for each panel
        for tabs in template.main[0][0]:
            for panel in tabs:
                #print(panel)
                # set param loading indicator param in panel to True
                panel[0].loading = True        

        # Create thread to run update_dashboard and pass template and date_picker as arguments
        t = threading.Thread(target=update_dashboard, args=(template, date_picker))
        t.daemon = False
        t.start()


def destroyed(session_context):
    print("Session destroyed", session_context)
    gc.collect()
    
    if  not restart_server_thread_task.is_alive():
        print("Starting restart server thread...")
        restart_server_thread_task.start()
    
def get_user_dashboard():

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
    # The app can be executed with -p argument to indicates the port from the command line: example: python3 app.py -p 5007

    default_port = 5006

    args = sys.argv[1:]

    if len(args) > 0:
        port_cmd_arg = args[0]
        port = args[1]

        # Check if port is a number
        if port_cmd_arg == '-p' and port.isdigit():
            port = int(port)
        else:  # Error in command line arguments
            print("Error indicating arguments or port. Using default port 5006")
            port = default_port
    else:
        port = default_port
    pn.serve(get_user_dashboard, port=port, show=False, static_dirs={'images': './images'}, admin=True, title='Clusco Reports',
             threaded=True, n_threads=4, check_unused_sessions_milliseconds=5000, unused_session_lifetime=5000, log_level='debug')
