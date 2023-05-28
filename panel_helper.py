"""
HoloViz panels management module
"""
import panel as pn

import plot_helper
import dashboard_utils

def create_plot_panel(df, title, id_var, var_name, value_name, xlabel, ylabel, cmap, climit, template, show_loading_msg=True):

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