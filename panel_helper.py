"""
HoloViz panels management module. We are using Panel (<https://panel.holoviz.org>) to create the dashboard panels.
"""
import panel as pn

# Application modules
import plot_helper
import dashboard_utils

def create_plot_panel(df, title, id_var, var_name, value_name, xlabel, ylabel, cmap, climit, template, show_loading_msg=True):
    """
    Creates a plot panel for a given dataframe and appends a plot using `plot_helper.multiplot_grouped_data`

    Parameters
    ----------
    - `df` (pandas.DataFrame) The dataframe to plot
    - `title` (str) The title of the plot
    - `id_var` (str) The name of the column to use as the id variable
    - `var_name` (str) The name of the column to use as the variable name
    - `value_name` (str) The name of the column to use as the value name
    - `xlabel` (str) The label for the x-axis
    - `ylabel` (str) The label for the y-axis
    - `cmap` (str) The name of the colormap to use
    - `climit` (tuple) The limits of the colorbar
    - `template` (panel.Template) The dashboard template
    - `show_loading_msg` (bool) Whether to show the loading message or not (Only when creating the plot for the first time)

    Returns
    -------
    - `plot_panel` (panel.Panel) The panel with columns whith the plot and the associated widget.
    """

    if show_loading_msg:
        dashboard_utils.update_loading_message(template, f'''<h1 style="text-align:center">Making plots...</h1> <h2 style="text-align:center">({title.split(' (')[0]})</h2> ''')

    print("   - Creating plot panel for: " + title)

    if (df.empty):
        print("   - No data to plot for: " + title)
        plot = plot_helper.create_empty_plot()
        plot_panel = pn.panel(plot, sizing_mode='stretch_width', linked_axes=False)
    
    else:
        plot = plot_helper.multiplot_grouped_data(df, id_var, value_name,
                        title, xlabel, ylabel, var_name, cmap, climit)

        c_widget = pn.widgets.DiscreteSlider
        c_widget.align = 'center'
        c_widget.sizing_mode = 'stretch_width'
        # c_widget.width_policy = ''

        plot_panel = pn.panel(plot, widget_location='bottom', widgets={
                            var_name: c_widget}, sizing_mode='stretch_width', linked_axes=False)
        
    return plot_panel



def create_l1_rate_plot_panel(data_dict, title, id_var, var_name, value_name, xlabel, ylabel, cmap, climit, template, show_loading_msg=True):
    """
    Creates a plot panel for the L1 rate and appends the plot and the widget using `plot_helper.plot_l1_rate_data`

    Parameters
    ----------
    - `data_dict` (dict) The dictionary containing the dataframes to plot
    - `title` (str) The title of the plot
    - `id_var` (str) The name of the column to use as the id variable (x axis of the plot)
    - `var_name` (str) The name of the column to use as the variable name
    - `value_name` (str) The name of the column to use as the value name (y axis of the plot)
    - `xlabel` (str) The label for the x-axis
    - `ylabel` (str) The label for the y-axis
    - `cmap` (LinearSegmentedColormap): The colormap object based on lookup tables using linear segments.
    - `climit` (tuple) The limits of the colorbar
    - `template` (panel.Template) The dashboard template
    - `show_loading_msg` (bool) Whether to show the loading message or not (Only when creating the plot for the first time)

    Returns
    -------
    - `plot_panel` (panel.Panel) The panel with columns whith the plot and the associated widget.

    """
    l1_rate_data = data_dict['l1_rate']

    if show_loading_msg:
        dashboard_utils.update_loading_message(template, f'''<h1 style="text-align:center">Making plots...</h1> <h2 style="text-align:center">({title.split(' (')[0]})</h2> ''')

    print("   - Creating plot panel for: " + title)

    if (l1_rate_data.empty):
        print("   - No data to plot for: " + title)
        plot = plot_helper.create_empty_plot()
        plot_panel = pn.panel(plot, sizing_mode='stretch_width', linked_axes=False)
    
    else:
        plot = plot_helper.plot_l1_rate_data(data_dict, id_var, value_name,
                        title, xlabel, ylabel, var_name, cmap, climit)

        c_widget = pn.widgets.DiscreteSlider
        c_widget.align = 'center'
        c_widget.sizing_mode = 'stretch_width'

        plot_panel = pn.panel(plot, widget_location='bottom', widgets={
                            var_name: c_widget}, sizing_mode='stretch_width', linked_axes=False)

    return plot_panel


def create_l0_ipr_plot_panel(data_dict, title, id_var, var_name, value_name, xlabel, ylabel, cmap, climit, template, show_loading_msg=True):
    """
    Creates a plot panel for the L0 pixel IPR and appends the plot and the widget using `plot_helper.plot_l0_ipr_data`

    Parameters
    ----------
    - `data_dict` (dict) The dictionary containing the data to plot (See `plot_helper.plot_l0_ipr_data` for more info)
    - `title` (str) The title of the plot
    - `id_var` (str) The name of the column to use as the id variable (x axis of the plot)
    - `var_name` (str) The name of the column to use as the variable name (channel, module...)
    - `value_name` (str) The name of the column to use as the value name (y axis of the plot)
    - `xlabel` (str) The label for the x-axis
    - `ylabel` (str) The label for the y-axis
    - `cmap` (LinearSegmentedColormap): The colormap object based on lookup tables using linear segments.
    - `climit` (tuple) The limits of the colorbar
    - `template` (panel.Template) The dashboard template
    - `show_loading_msg` (bool) Whether to show the loading message or not (Only when creating the plot for the first time)

    Returns
    -------
    - `plot_panel` (panel.Panel) The panel with columns whith the plot and the associated widget.

    """
    l0_ipr_data = data_dict['l0_pixel_ipr']

    if show_loading_msg:
        dashboard_utils.update_loading_message(template, f'''<h1 style="text-align:center">Making plots...</h1> <h2 style="text-align:center">({title.split(' (')[0]})</h2> ''')

    print("   - Creating plot panel for: " + title)

    if (l0_ipr_data.empty):
        print("   - No data to plot for: " + title)
        plot = plot_helper.create_empty_plot()
        plot_panel = pn.panel(plot, sizing_mode='stretch_width', linked_axes=False)
    
    else:
        plot = plot_helper.plot_l0_ipr_data(data_dict, id_var, value_name,
                        title, xlabel, ylabel, var_name, cmap, climit)

        c_widget = pn.widgets.DiscreteSlider
        c_widget.align = 'center'
        c_widget.sizing_mode = 'stretch_width'

        plot_panel = pn.panel(plot, widget_location='bottom', widgets={
                            var_name: c_widget}, sizing_mode='stretch_width', linked_axes=False)

    return plot_panel



def create_tib_rates_plot_panel(data_dict, title, xlabel, ylabel, template, show_loading_msg=True):
    """
    Creates a plot panel for the TIB rates and appends the plot using `plot_helper.plot_tib_rate_data`

    Parameters
    ----------
    - `data_dict` (dict) The dictionary containing the data to plot (See `plot_helper.plot_tib_rate_data` for more info)
    - `title` (str) The title of the plot
    - `xlabel` (str) The label for the x-axis
    - `ylabel` (str) The label for the y-axis
    - `template` (panel.Template) The dashboard template
    - `show_loading_msg` (bool) Whether to show the loading message or not (Only when creating the plot for the first time)

    Returns
    -------
    - `plot_panel` (panel.Panel) The panel with columns whith the plot and the associated widget.
    """

    tib_busy_data = data_dict['tib_busy_rate']
    if show_loading_msg:
        dashboard_utils.update_loading_message(template, f'''<h1 style="text-align:center">Making plots...</h1> <h2 style="text-align:center">({title.split(' (')[0]})</h2> ''')

    print("   - Creating plot panel for: " + title)

    if (tib_busy_data.empty):
        print("   - No data to plot for: " + title)
        plot = plot_helper.create_empty_plot()
        plot_panel = pn.panel(plot, sizing_mode='stretch_width', linked_axes=False)
        
    else:
        plot = plot_helper.plot_tib_rate_data(data_dict, title, xlabel, ylabel)

        plot_panel = pn.Column(plot, sizing_mode='stretch_width')
    return plot_panel


def create_dragon_busy_plot_panel(data, title, xlabel, ylabel, template, show_loading_msg=True):
    """
    Creates a plot panel for the Dragon busy and appends the plot using `plot_helper.plot_dragon_busy_data`

    Parameters
    ----------
    - `data` (pandas.DataFrame) The data to plot
    - `title` (str) The title of the plot
    - `xlabel` (str) The label for the x-axis
    - `ylabel` (str) The label for the y-axis
    - `template` (panel.Template) The dashboard template
    - `show_loading_msg` (bool) Whether to show the loading message or not (Only when creating the plot for the first time)
    """
    if show_loading_msg:
        dashboard_utils.update_loading_message(template, f'''<h1 style="text-align:center">Making plots...</h1> <h2 style="text-align:center">({title.split(' (')[0]})</h2> ''')

    print("   - Creating plot panel for: " + title)

    if (data.empty):
        print("   - No data to plot for: " + title)
        plot = plot_helper.create_empty_plot()
        plot_panel = pn.panel(plot, sizing_mode='stretch_width', linked_axes=False)
        
    else:
        plot = plot_helper.plot_dragon_busy_data(data, title, xlabel, ylabel)

        plot_panel = pn.panel(plot, sizing_mode='stretch_width', linked_axes=False)

    return plot_panel