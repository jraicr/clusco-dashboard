# Description: This file contains the main application code for the CLUSCO dashboard

import pandas as pd
import holoviews as hv # noqa
import holoviews.operation.datashader as hd # noqa
import datashader as ds # noqa
import hvplot.pandas # noqa
import panel as pn
import time
import gc
import threading
import sys
import os
from config import WEBSOCKET_ORIGIN

# Application modules
import dashboard_utils


gc.enable()

# Holoviz and Pandas Settings
hv.extension('bokeh', logo=False) 
pn.extension(loading_spinner='dots', loading_color='#00204e', sizing_mode="stretch_width")
pd.options.plotting.backend = 'holoviews'
pn.config.throttled = True
# pn.config.sizing_mode = 'stretch_width'


def restart_server_if_empty_task(interval_sec=60):
    # This function will restart the server if it is empty
    while True:
        time.sleep(interval_sec)
        
        #print('Checking if server is empty to perform restart...')
        if pn.state.session_info['live'] == 0:
            print("Restarting server to clean up resources, as there are 0 active sessions. ")
            #os.execv(sys.executable, ['python -u'] + sys.argv + ['>> output.log'])
            os.execv(sys.executable, ['python'] + sys.argv)
        
# Global variable to store the thread that will restart the server if it is empty
restart_server_thread_task = threading.Thread(target=restart_server_if_empty_task)
restart_server_thread_task.daemon = True


def destroyed(session_context):
    print("Session destroyed", session_context)
    gc.collect()
    
    if  not restart_server_thread_task.is_alive():
        print("Starting application restart checker in another thread")
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

    create_dashboard_thread_task = threading.Thread(target=dashboard_utils.create_dashboard, args=(material_dashboard, ))
    create_dashboard_thread_task.daemon = True
    create_dashboard_thread_task.start()
    
    return material_dashboard


if __name__ == '__main__':
    # The app can be executed with -p argument to indicates the port from the command line: example: python3 app.py -p 5007

    default_port = 5006

    args = sys.argv[1:]

    if len(args) > 1:
        port_cmd_arg = args[0]

        # Check if port is a number
        if port_cmd_arg == '-p':
            port = args[1]
            if port.isdigit():
                port = int(port)
        
        else:  # Error in command line arguments
            print("Error indicating arguments or port. Using default port 5006")
            port = default_port
    else:
        port = default_port
    

    pn.serve(get_user_dashboard, address='127.0.0.1', port=port, websocket_origin=WEBSOCKET_ORIGIN, show=False, static_dirs={'images': './images'}, admin=True, title='Clusco Reports',
             threaded=True, n_threads=4)