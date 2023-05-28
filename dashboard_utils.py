import panel as pn
import datetime as dt
import time
import threading
from matplotlib.colors import LinearSegmentedColormap

import database
import panel_helper
from config import DB_HOST, DB_PORT, DB_NAME

"""
Module with utility functions for the dashboard.
"""

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


def create_dashboard(template, date_filter=dt.date.today(), update=False):
    """
    Creates the dashboard with all the panel plots with the given template and date filter and shows it in the browser.

    Parameters
    ----------
    - `template` (pn.template.MaterialTemplate) The template to use for the dashboard.
    - `date_filter` (date) The date filter to use for the dashboard. Defaults to today.
    - `update` (bool) Whether to update the dashboard or is a new creation. Defaults to False.

    """

    if update:
        print('Updating dashboard')
    else:
        print('Creating dashboard')
        
    tic = time.perf_counter()

    pn.param.ParamMethod.loading_indicator = True

    if update is False:
        update_loading_message(template, '''<h1 style="text-align:center">Getting data...</h1>''')

    # Setup BD Connection
    db = database.connect(DB_HOST, DB_PORT, DB_NAME)

    if (db == None):
        display_database_error(template=template)
        exit()

    clusco_min_collection = db['CLUSCO_min']
    tib_min_collection = db['TIB_min'] 

    # Data retrieved from database
    pacta_temperature_data = database.get_data_by_date(collection=clusco_min_collection, property_name='scb_pixel_temperature',
                                            date_time=date_filter, value_field='avg', id_var='date', var_name='channel', value_name='temperature', search_previous = not update)

    # Get the min date (year-month-day) from the PACTA temperature dataframe, where date is the index of the dataframe,
    # in case the dataframe is not empty. We are going to retrieve the data from the
    # left plots from the first date with available data in the PACTA temperature data
    if not pacta_temperature_data.empty:
        min_filtered_date = pacta_temperature_data.index.min().date()
    else:
        min_filtered_date = date_filter

    scb_temperature_data = database.get_data_by_date(collection=clusco_min_collection, property_name='scb_temperature',
                                            date_time=min_filtered_date, value_field='avg', id_var='date', var_name='module', value_name='temperature', search_previous = False)

    scb_humidity_data = database.get_data_by_date(collection=clusco_min_collection, property_name='scb_humidity',
                                        date_time=min_filtered_date, value_field='avg', id_var='date', var_name='module', value_name='humidity', search_previous = False)

    scb_anode_current_data = database.get_data_by_date(collection=clusco_min_collection, property_name='scb_pixel_an_current',
                                            date_time=min_filtered_date, value_field='avg', id_var='date', var_name='channel', value_name='anode', search_previous = False)

    hv_data = database.get_data_by_date(collection=clusco_min_collection, property_name='scb_pixel_hv_monitored',
                            date_time=min_filtered_date, value_field='avg', id_var='date', var_name='channel', value_name='hv', search_previous = False)
    
    scb_backplane_temperature_data = database.get_data_by_date(collection=clusco_min_collection, property_name='backplane_temperature',
                                            date_time=min_filtered_date, value_field='avg', id_var='date', var_name='module', value_name='temperature', search_previous = False)


    # L1 Rate plot data
    l1_rate_data = database.get_data_by_date(collection=clusco_min_collection, property_name='l1_rate',
                                            date_time=min_filtered_date, value_field='avg', id_var='date', var_name='module', value_name='l1_rate', search_previous = False)
    
    l1_rate_control_data = database.get_scalar_data_by_date(collection=clusco_min_collection, property_name='clusco_l1_rate_control',
                                            date_time=min_filtered_date, value_field='avg', value_name='l1_rate_control', search_previous = False, remove_zero_values=True)

    l0_rate_control_data = database.get_scalar_data_by_date(collection=clusco_min_collection, property_name='clusco_l0_rate_control',
                                            date_time=min_filtered_date, value_field='avg', value_name='l0_rate_control', search_previous = False, remove_zero_values=True)
    
    l1_rate_max_data = database.get_scalar_data_by_date(collection=clusco_min_collection, property_name='clusco_l1_rate_max',
                                            date_time=min_filtered_date, value_field='avg', value_name='l1_rate_max', search_previous = False)
    
    l1_rate_target_data = database.get_scalar_data_by_date(collection=clusco_min_collection, property_name='clusco_l1_rate_target',
                                                           date_time=min_filtered_date, value_field='avg', value_name='l1_rate_target', search_previous = False)

    # L0 Pixel Ipr Data
    l0_pixel_ipr_data = database.get_data_by_date(collection=clusco_min_collection, property_name='l0_pixel_ipr',
                                                   date_time=min_filtered_date, value_field='avg', id_var='date', var_name='channel', value_name='l0_pixel_ipr', search_previous = False)
    # L0 Rate max data
    l0_rate_max_data = database.get_scalar_data_by_date(collection=clusco_min_collection, property_name='clusco_l0_rate_max', 
                                                        date_time=min_filtered_date, value_field='avg', value_name='l0_rate_max', search_previous = False)

    # TIB rates data
    tib_busy_rate_data = database.get_scalar_data_by_date(collection=tib_min_collection, property_name='TIB_Rates_BUSYRate', date_time=min_filtered_date, value_field='avg', value_name='tib_busy_rate', search_previous = False)
    tib_calibration_rate_data = database.get_scalar_data_by_date(collection=tib_min_collection, property_name='TIB_Rates_CalibrationRate', date_time=min_filtered_date, value_field='avg', value_name='calibration_rate', search_previous = False)
    tib_camera_rate_data = database.get_scalar_data_by_date(collection=tib_min_collection, property_name='TIB_Rates_CameraRate', date_time=min_filtered_date, value_field='avg', value_name='camera_rate', search_previous = False)
    tib_local_rate_data = database.get_scalar_data_by_date(collection=tib_min_collection, property_name='TIB_Rates_LocalRate', date_time=min_filtered_date, value_field='avg', value_name='local_rate', search_previous = False)
    tib_pedestal_rate_data = database.get_scalar_data_by_date(collection=tib_min_collection, property_name='TIB_Rates_PedestalRate', date_time=min_filtered_date, value_field='avg', value_name='pedestal_rate', search_previous = False)
    
    # Dragon Busy
    dragon_busy_data = database.get_data_by_date(collection=clusco_min_collection, property_name='dragon_busy', date_time=min_filtered_date, value_field='max', id_var='date', var_name='module', value_name='busy_status', search_previous = False)

    # close mongodb connection
    db.client.close()

    if update is False:
        update_loading_message(template, '''<h1 style="text-align:center">Making plots...</h1>''')

    if update is False:
        # min_date_from_data = min([x for x in data_min_date_dict.values() if x is not None])

        date_picker = pn.widgets.DatePicker(
            name='Date Selection', value=min_filtered_date, end=dt.date.today())

    print("\nMaking plots...")

    # # # # # # # # # # # # #
    # First dashboard tab  #
    # # # # # # # # # # # # #
    pacta_temp_title = 'PACTA Temperature ' + '(' + str(min_filtered_date) + ')'
    pacta_temp_plot_panel =  panel_helper.create_plot_panel(pacta_temperature_data, pacta_temp_title, 'date', 'channel', 'temperature', 'Time (UTC)', 'Temperature (ºC)', cmap_temps, (0, 30), template, not update) 

    if update:
        template.main[0][0][0][0, 0] = pacta_temp_plot_panel

    scb_temp_title = 'SCB Temperature ' + '(' + str(min_filtered_date) + ')'
    scb_temp_plot_panel = panel_helper.create_plot_panel(scb_temperature_data, scb_temp_title, 'date', 'module', 'temperature', 'Time (UTC)', 'Temperature (ºC)', cmap_temps, (0, 30), template, not update)

    if update:
        template.main[0][0][0][0, 1] = scb_temp_plot_panel

    scb_humidity_title = 'SCB Humidity ' + '(' + str(min_filtered_date) + ')'
    scb_humidity_plot_panel = panel_helper.create_plot_panel(scb_humidity_data, scb_humidity_title, 'date', 'module', 'humidity', 'Time (UTC)', 'Humidity (%)', cmap_humidty, (0, 80), template, not update)

    if update:
        template.main[0][0][0][0, 2] = scb_humidity_plot_panel
    
    scb_anode_title = 'SCB Anode Current ' + '(' + str(min_filtered_date) + ')'
    scb_anode_current_plot_panel = panel_helper.create_plot_panel(scb_anode_current_data, scb_anode_title, 'date', 'channel', 'anode', 'Time (UTC)', 'Anode Current (µA)', cmap_anode, (0, 100), template, not update)

    if update:
        template.main[0][0][0][1, 0] = scb_anode_current_plot_panel

    hv_title = 'High Voltage ' + '(' + str(min_filtered_date) + ')'
    hv_plot_panel = panel_helper.create_plot_panel(hv_data, hv_title, 'date', 'channel', 'hv', 'Time (UTC)', 'HV (V)', cmap_hv, (10, 1400), template, not update)
    
    if update:
        template.main[0][0][0][1, 1] = hv_plot_panel
    
    
    scb_backplane_temp_title = 'SCB Backplane Temperature ' + '(' + str(min_filtered_date) + ')'
    scb_backplane_temp_plot_panel = panel_helper.create_plot_panel(scb_backplane_temperature_data, scb_backplane_temp_title, 'date', 'module', 'temperature', 'Time (UTC)', 'Temperature (ºC)', cmap_backplane_temp, (0, 37), template, not update)

    if update:
        template.main[0][0][0][1, 2] = scb_backplane_temp_plot_panel


    # # # # # # # # # # # # #
    # Second dashboard tab  #
    # # # # # # # # # # # # #

    # Creates a dict with all the params to plot with the L1 Rate data
    l1_rate_data_dict = {'l1_rate': l1_rate_data,
                         'l1_rate_control': l1_rate_control_data,
                         'l1_rate_max': l1_rate_max_data,
                         'l1_rate_target': l1_rate_target_data,
                         'l0_rate_control': l0_rate_control_data}

    l1_rate_title = 'L1 Rate ' + '(' + str(min_filtered_date) + ')'
    l1_rate_plot_panel = panel_helper.create_l1_rate_plot_panel(l1_rate_data_dict,
                l1_rate_title, 'date', 'module', 'l1_rate', 'Time (UTC)', 'L1 Rate (Hz)', cmap_temps, (0, 1000), template, not update)

    if update:
        template.main[0][0][1][0, 0] = l1_rate_plot_panel

    # Creates a dict with all the params to plot with the L0 Pixel IPR data
    l0_pixel_ipr_data_dict = {'l0_pixel_ipr': l0_pixel_ipr_data,
                              'l0_rate_max': l0_rate_max_data}

    l0_pixel_ipr_title = 'L0 Pixel IPR ' + '(' + str(min_filtered_date) + ')'
    l0_pixel_ipr_panel = panel_helper.create_l0_ipr_plot_panel(l0_pixel_ipr_data_dict, l0_pixel_ipr_title, 'date', 'channel', 'l0_pixel_ipr', 'Time (UTC)', 'L0 Pixel IPR (Hz)', cmap_temps, (0, 1000), template, not update)

    if update:
        template.main[0][0][1][0, 1] = l0_pixel_ipr_panel

    # Creates a dict with all the params to plot with the TIB Rates
    tib_rates_data_dict = {'tib_busy_rate': tib_busy_rate_data,
                            'tib_calibration_rate': tib_calibration_rate_data,
                            'tib_camera_rate': tib_camera_rate_data,
                            'tib_local_rate': tib_local_rate_data,
                            'tib_pedestal_rate': tib_pedestal_rate_data}
    
    tib_rates_title = 'TIB Rates ' + '(' + str(min_filtered_date) + ')'
    tib_rates_panel = panel_helper.create_tib_rates_plot_panel(tib_rates_data_dict, tib_rates_title, 'Time (UTC)', 'TIB Rates (Hz)', template, not update)

    if update:
        template.main[0][0][1][1, :] = tib_rates_panel

    # # # # # # # # # # # # #
    # Third dashboard tab   #
    # # # # # # # # # # # # #

    dragon_busy_title = 'Dragon Busy ' + '(' + str(min_filtered_date) + ')'
    dragon_busy_panel = panel_helper.create_dragon_busy_plot_panel(dragon_busy_data, dragon_busy_title, 'Time (UTC)', 'Module ID', template, not update)

    if update:
        template.main[0][0][2][0, :] = dragon_busy_panel


    if update is False:
        update_loading_message(template, '''<h1 style="text-align:center">Deploying dashboard...</h1>''')
    
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
        grid_b = pn.GridSpec(sizing_mode='stretch_both', ncols=2, nrows=2, mode='override')
        grid_b[0, 0] = l1_rate_plot_panel
        grid_b[0, 1] = l0_pixel_ipr_panel
        grid_b[1, :] = tib_rates_panel
        
        # =========================
        # THIRD GRID - THIRD TAB
        # =========================
        grid_c = pn.GridSpec(sizing_mode='stretch_both', ncols=1, nrows=1, mode='override')
        grid_c[0, 0] = dragon_busy_panel
        

        # Sidebar creation

        date_selection_info = """### Observations about date selection \n Please note that when selecting a date, the graphs will display data from 12:00 pm\
              on the selected day until 12:00 pm the following day. If you wish to view data from before 12:00 pm on\
              the selected day, you should select the previous day."""
        
        png_pane = pn.pane.PNG('./images/cta-logo.png', width=200, align='center')
        sidebar_col = pn.Column(pn.layout.HSpacer(), png_pane,
                                pn.layout.HSpacer(), date_picker,
                                date_selection_info)

        # Append tabs and grids to template main
        template.main[0].sizing_mode = 'stretch_both'
        
        # Creating tabs and appends grids to it
        tabs = pn.Tabs(('Pixel Temp, Anode & HV - SCB Temp & Humidity', grid), ('Rates', grid_b), ('Dragon Busy', grid_c))

        template.main[0][0] = tabs

        # Append content to template sidebar
        template.sidebar.objects[0].sizing_mode = 'stretch_both'
        template.sidebar[0][0] = sidebar_col

        @pn.depends(date_picker.param.value, watch=True)
        def thread_update_dashboard_task(date_picker):

            # Iterates each tab to activates loading indicator for each panel
            for tabs in template.main[0][0]:
                for panel in tabs:
                    #print(panel)
                    # set param loading indicator param in panel to True
                    panel[0].loading = True        

            # Create thread to update the dashboard
            t = threading.Thread(target=create_dashboard, args=(template, date_picker, True))
            t.daemon = False
            t.start()
    
    toc = time.perf_counter()
    print(f"\Dashboard deployed in {toc - tic:0.4f} seconds")


def update_loading_message(template:pn.template.MaterialTemplate, message:str):
    """
    Updates and shows a loading message in the dashboard while deploying it for the first time.

    Parameters
    ----------
    - `template` (pn.template.MaterialTemplate) The template object from panel.
    - `message` (str) The message to display in the loading screen.
    """
    template.main[0][0][2].object = message
    

def display_database_error(template: pn.template.MaterialTemplate):
    """
    Display a message in the dashboard when the connection to the database fails.

    Parameters
    ----------
    - `template` (pn.template.MaterialTemplate) The template object from panel.
    """
    template.main[0][0] = pn.Column()
    template.sidebar[0][0] = pn.Column()
    db_error_image = pn.pane.PNG('./images/db_error.png', width=100, align='center')
    db_error_text = pn.pane.Markdown('''<h1 style="text-align:center">Connection to database failed. Check if database is running or contact with a system administrator.</h1>''')
    main_error_col = pn.Column(pn.layout.VSpacer(), db_error_image,  db_error_text, pn.layout.VSpacer(), sizing_mode='stretch_both')
    template.main[0][0] = main_error_col
