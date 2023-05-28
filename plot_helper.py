"""
HoloViz plots management module
"""
import pandas as pd
import holoviews as hv # noqa


def hvplot_df_line(df:pd.DataFrame, x:str, y:str, title:str, dic_opts:dict, color:str='green'):
    """
    Plots a line graph - using hvPlot - from a pandas dataframe.

    Parameters
    ----------
    - `df` (pandas.DataFrame): The dataframe to plot.
    - `x` (str): The name of the column to use as x axis.
    - `y` (str): The name of the column to use as y axis.
    - `title` (str): The title of the plot.
    - `dic_opts` (dict): A dictionary with the options to pass to the plot created with hvPlot. See more at <https://hvplot.holoviz.org/user_guide/Customization.html>
    - `color` (str): The color of the line. Green by default.

    Returns
    ----------
    - `dynamic_map` (holoviews.core.spaces.DynamicMap): The holoviews Dynamic map created with hvPlot.
    """
    dynamic_map = df.hvplot.line(x=x, y=y, title=title, color=color, hover_cols='all',
                                 responsive=True, min_height=400, muted_alpha=0)

    
    options = list(dic_opts.items())
    dynamic_map.opts(**dict(options))

    return dynamic_map


def hvplot_df_grouped_line(df:pd.DataFrame, x, y, title:str, dic_opts:dict, groupby:str, color:str='green'):
    """
    Plots a line graph - using hvPlot - from a pandas dataframe grouping by a column in the dataframe. hvPlot will create a widget to select the group to plot.

    Parameters
    ----------
    - `df` (pandas.DataFrame): The dataframe to plot.
    - `x` (str): The name of the column to use as x axis.
    - `y` (str): The name of the column to use as y axis.
    - `title` (str): The title of the plot.
    - `dic_opts` (dict): A dictionary with the options to pass to the plot created with hvPlot. See more at <https://hvplot.holoviz.org/user_guide/Customization.html>
    - `groupby` (str): The name of the column to use to group the dataframe.
    - `color` (str): The color of the line.

    Returns
    ----------
    - `dynamic_map` (holoviews.core.spaces.DynamicMap): A DynamicMap instance from Holoviews created with hvPlot. See more at <https://holoviews.org/reference/containers/plotly/DynamicMap.html>
    """
    
    dynamic_map = df.hvplot.line(x=x, y=y, title=title, color=color, groupby=groupby, hover_cols='all',
                                 label='selected ' + groupby,  responsive=True, min_height=400, muted_alpha=0)

    options = list(dic_opts.items())
    dynamic_map.opts(**dict(options))

    return dynamic_map



def hvplot_df_max_min_avg_line(df, x, title, dic_opts, category):
    """
     
    Plots lines graphs - using hvPlot - from a pandas dataframe with max, min and avg columns

    Parameters
    ----------
    - `df` (pandas.DataFrame): The dataframe to plot with max, min and avg columns.
    - `x` (str): The name of the column to use as x axis.
    - `title` (str): The title of the plot.
    - `dic_opts` (dict): A dictionary with the options to pass to the plot created with hvPlot. See more at <https://hvplot.holoviz.org/user_guide/Customization.html>
    - `category` (str): The name of the variable to plot (channel, module...)

    Returns
    ----------
    - `composite_plots` (holoviews.core.overlay.Overlay): The composited plots created with hvPlot.
    """
    
    max_line = df.hvplot.line(x=x, y='max', title=title, responsive=True, min_height=400, hover_cols=['x', 'y', 'max_' + category], label='max', color='red').opts(alpha=1, muted_alpha=0)
        
    min_line = df.hvplot.line(x=x, y='min', title=title, responsive=True, min_height=400, hover_cols=['x', 'y', 'min_' + category], label='min', color='blue').opts(alpha=1, muted_alpha=0)
    
    mean_line = df.hvplot.line(x=x, y='avg', title=title, responsive=True, min_height=400, hover_cols=['x', 'y'], label='avg', color='black').opts(alpha=1, muted_alpha=0)
    
    options = list(dic_opts.items())
    
    composite_plots = max_line * mean_line * min_line
    composite_plots.opts(**dict(options))
    composite_plots

    return composite_plots



def hvplot_df_scatter(df, x, y, title, color, size, marker, dic_opts, cmap="reds", groupby=None, datashade=False, rasterize=False, dynamic=True):
    """
    Plots a scatter graph - using hvPlot - from a pandas dataframe.

    Parameters
    ----------
    - `df` (pandas.DataFrame): The dataframe to plot.
    - `x` (str): The name of the dataframe column to use as x axis.
    - `y` (str): The name of the dataframe column to use as y axis.
    - `title` (str): The title of the plot.
    - `color` (str): The name of the dataframe column to use to color the points.
    - `size` (int): The size of the points.
    - `marker` (str): The marker of the points.
    - `dic_opts` (dict): A dictionary with the options to pass to the plot created with hvPlot. See more at <https://hvplot.holoviz.org/user_guide/Customization.html>
    - `cmap` (str): The color map to use to color the points.
    - `groupby` (str): The name of the dataframe column to use to group the points.
    - `datashade` (bool): Whether to use datashade or not.
    - `rasterize` (bool): Whether to use rasterize or not.
    - `dynamic` (bool): Whether to use dynamic or not.

    Returns
    ----------
    - `plot` (holoviews.core.spaces.DynamicMap): A DynamicMap instance from Holoviews created with hvPlot. See more at <https://holoviews.org/reference/containers/plotly/DynamicMap.html>
    """
    if rasterize == False:
        plot = df.hvplot.scatter(x=x, y=y, title=title, color=color, label='selected ' + groupby,
                                 size=size, marker=marker, cmap=cmap, groupby=groupby, datashade=datashade, rasterize=rasterize, dynamic=dynamic, responsive=True, min_height=400, muted_alpha=0)

    else:
        plot = df.hvplot.scatter(x=x, y=y, title=title, color=color,
                                 marker=marker, cmap=cmap, groupby=groupby, datashade=datashade, rasterize=rasterize, dynamic=dynamic, responsive=True, min_height=400)

    options = list(dic_opts.items())
    plot.opts(**dict(options))

    return plot


def create_empty_plot():
    
    """
    Creates an empty plot with a text indicating that there is no data available in the selected date.

    Returns
    ----------
    - `empty_plot` (holoviews.core.spaces.DynamicMap): A DynamicMap instance from Holoviews created with hvPlot. See more at <https://holoviews.org/reference/containers/plotly/DynamicMap.html>
    """
    empty_plot = hv.Curve([])

    # Add text the plot indicating "There is no available data in the selected date"
    empty_plot = empty_plot * \
        hv.Text(0.6, 0.6, 'There is no available data in the selected date')

    return empty_plot



def build_min_max_avg(df, x, y, category):
    """
    Builds a dataframe with the max, min and avg values for each date.

    Parameters
    ----------
    - `df` (pandas.DataFrame): The dataframe to process.
    - `x` (str): The name of the column to use as x axis.
    - `y` (str): The name of the column to use as y axis.
    - `category` (str): The name of the variable to plot (channel, module...)

    Returns
    ----------
    - `df_agg` (pandas.DataFrame): The dataframe with the max, min and avg values for each date.
    
    """
    def process_chunk(df_chunk, x, y, category):
        """
        Processes a chunk of the dataframe.

        Parameters
        ----------
        - `df_chunk` (pandas.DataFrame): The chunk of the dataframe to process.
        - `x` (str): The name of the column to use as x axis.
        - `y` (str): The name of the column to use as y axis.
        - `category` (str): The name of the variable to plot (channel, module...)

        Returns
        ----------
        - `df_agg` (pandas.DataFrame): The chunked dataframe with the max, min and avg values for each date.
        """
        
        # Exclude the last date from the chunk
        last_date = df_chunk.index[-1]
        df_chunk = df_chunk[df_chunk.index != last_date]

        # Group by date and get the max, min and avg values
        df_agg = df_chunk.groupby(x).agg(
            max=(y, 'max'),
            min=(y, 'min'),
            mean=(y, 'mean'),
        ).rename(columns={'mean': 'avg'}).reset_index()

        # Merge with the original DataFrame to get the category for the max and min values
        df_agg = df_agg.merge(
            df_chunk.reset_index(),
            left_on=[x, 'max'],
            right_on=[x, y],
            how='left'
        ).rename(columns={category: 'max_' + category}).drop(columns=y)

        df_agg = df_agg.merge(
            df_chunk.reset_index(),
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

    # Split the data into chunks and process each chunk separately .
    # This is useful to avoid RAM memory issues, since the dataframe could be too big while processing data.
    chunksize = 300000
    result = []
    remaining_rows = pd.DataFrame()
    for i in range(0, len(df), chunksize):
        df_chunk = pd.concat([remaining_rows, df.iloc[i:i+chunksize]])
        result.append(process_chunk(df_chunk, x, y, category))
        
        # Save the remaining rows for the next iteration 
        # Useful to avoid losing the last date of the chunk and to avoid having duplicated dates in rows in the next chunk
        if i+chunksize-1 < len(df):
            last_date = df.iloc[i+chunksize-1].name
        else:
            last_date = df.iloc[-1].name
        remaining_rows = df[df.index == last_date]

    # Process the remaining rows
    result.append(process_chunk(remaining_rows, x, y, category))

    # Concatenate the results
    df_agg = pd.concat(result)

    return df_agg


def disable_logo(plot, element):
    """
    Hook to disable the Bokeh logo in the plot.

    Parameters
    ----------
    - `plot` (holoviews.plotting.bokeh.ElementPlot): The plot to disable the logo. (Automatically passed by HoloViews)
    - `element` (holoviews.element.chart.Chart): The element to disable the logo. (Automatically passed by HoloViews)
    """
    plot.state.toolbar.logo = None

def multiplot_grouped_data(data, x, y, title, xlabel, ylabel, groupby, cmap_custom, clim):
    """
    Composite Plot with:
      - max, min and average lines
      - line for the selected group (channel, module)
      - The scattered points corresponding to the selected group/var name (channel, module...)
      - The rasterized scattered points for all group/var name available in the dataframe (channels, modules...)

    Parameters
    ----------
    - `data` (pandas.DataFrame): The dataframe with the data to plot.
    - `x` (str): The name of the column to use as x axis.
    - `y` (str): The name of the column to use as y axis.
    - `title` (str): The title of the plot.
    - `xlabel` (str): The label of the x axis.
    - `ylabel` (str): The label of the y axis.
    - `groupby` (str): The name of the variable to plot (channel, module...)
    - `cmap_custom` (LinearSegmentedColormap): The colormap object based on lookup tables using linear segments.
    - `clim` (tuple): The min and max values for the colormap.

    Returns
    ----------
    - `composite_plot` (holoviews.core.overlay.Overlay): The composited plots created with hvPlot.
    """

    # Build a pandas dataframe from the original dataframe and select the min and max values for each date
    df_with_min_max_avg = build_min_max_avg(data, x, y, groupby)
    
    # Just for debugging purposes: Check if we have duplicated indexes (dates) in the dataframe
    # print(df_with_min_max_avg[df_with_min_max_avg.index.duplicated(keep=False)])

    max_line_plot = hvplot_df_max_min_avg_line(df_with_min_max_avg, x=x, title=title, dic_opts={
        'padding': 0.1, 'tools': ['hover'], 'xlabel': xlabel, 'ylabel': ylabel, 'axiswise': True, 'show_legend': True}, category=groupby)
    
    # Plot lines grouped by channel from data (channel is selected by widget and just one channel is shown at a time)
    lines_plot = hvplot_df_grouped_line(data, x=x, y=y, title=title, dic_opts={
        'padding': 0.1, 'tools': ['hover'], 'xlabel': xlabel, 'ylabel': ylabel, 'axiswise': True}, groupby=groupby, color='purple')

    # Plot scatter from data for a single channel
    single_channel_scatter_plot = hvplot_df_scatter(data, x=x, y=y, title=title, color=y, cmap=cmap_custom, size=15, marker='o', dic_opts={
        'padding': 0.1, 'tools': [''], 'xlabel': xlabel, 'ylabel': ylabel, 'clim': clim, 'alpha': 0.5}, groupby=groupby)

    # Plot scatter from data for all channels (rasterized)
    all_channels_scatter_plot = hvplot_df_scatter(data, x=x, y=y, title=title, color=y, cmap=cmap_custom,  size=20, marker='o', dic_opts={
                                                  'padding': 0.1, 'tools': [''], 'xlabel': xlabel, 'alpha': 0.15, 'ylabel': ylabel, 'clim': clim}, rasterize=True, dynamic=False)

    # Create a composite plot with all the plots merged
    composite_plot = lines_plot * single_channel_scatter_plot  * max_line_plot * all_channels_scatter_plot

    return composite_plot.opts(legend_position='top', responsive=True, min_height=500, hooks=[disable_logo], show_grid=True, legend_opts={"click_policy": "hide"},)



def plot_l1_rate_data(data_dict, x, y, title, xlabel, ylabel, groupby, cmap_custom, clim):
    """
    Composite plot for L1 rate data. It shows:
        - max, min and average lines
        - line for the selected channel
        - The scattered points corresponding to the selected channel
        - The rasterized scattered points for the values from all channels

        Other parameters beingh plotted with this data:
         - L1 Rate control
         - L1 Rate max
         - L1 Rate target
         - L0 Rate Control

    Parameters
    ----------
    - `data_dict` (dict): The dictionary with the data to plot. It should contain the following keys: 
        - `l1_rate` (pandas.DataFrame): The dataframe with the l1 rate data to plot.
        - `l1_rate_control` (pandas.DataFrame): The dataframe with the l1 rate control data to plot.
        - `l1_rate_max` (pandas.DataFrame): The dataframe with the l1 rate max data to plot.
        - `l1_rate_target` (pandas.DataFrame): The dataframe with the l1 rate target data to plot.
        - `l0_rate_control` (pandas.DataFrame): The dataframe with the l0 rate control data to plot.
    - `x` (str): The name of the column to use as x axis.
    - `y` (str): The name of the column to use as y axis.
    - `title` (str): The title of the plot.
    - `xlabel` (str): The label of the x axis.
    - `ylabel` (str): The label of the y axis.
    - `groupby` (str): The name of the variable to plot (channel, module...)
    - `cmap_custom` (LinearSegmentedColormap): The colormap object based on lookup tables using linear segments.
    - `clim` (tuple): The min and max values for the colormap.

    Returns
    ----------
    - `composite_plot` (holoviews.core.overlay.Overlay): The composited plots created with hvPlot.

    """
    l1_rate_data = data_dict['l1_rate']
    l1_rate_control_data = data_dict['l1_rate_control']
    l1_rate_max_data = data_dict['l1_rate_max']
    l1_rate_target_data = data_dict['l1_rate_target']
    l0_rate_control_data = data_dict['l0_rate_control']

    # Build a pandas dataframe from the original dataframe and select the min and max values for each date
    df_with_min_max_avg = build_min_max_avg(l1_rate_data, x, y, groupby)
    
    # Just for debugging purposes: Check if we have duplicated indexes (dates) in the dataframe
    # print(df_with_min_max_avg[df_with_min_max_avg.index.duplicated(keep=False)])

    max_line_plot = hvplot_df_max_min_avg_line(df_with_min_max_avg, x=x, title=title, dic_opts={
        'padding': 0.1, 'tools': ['hover'], 'xlabel': xlabel, 'ylabel': ylabel, 'axiswise': True, 'show_legend': True, 'responsive': True, 'min_height':400}, category=groupby)
    
    # Plot lines grouped by channel from data (channel is selected by widget and just one channel is shown at a time)
    lines_plot = hvplot_df_grouped_line(l1_rate_data, x=x, y=y, title=title, dic_opts={
        'padding': 0.1, 'tools': ['hover'], 'xlabel': xlabel, 'ylabel': ylabel, 'axiswise': True, 'responsive': True, 'min_height':400}, groupby=groupby, color='purple')

    # Plot scatter from data for a single channel
    single_channel_scatter_plot = hvplot_df_scatter(l1_rate_data, x=x, y=y, title=title, color=y, cmap=cmap_custom, size=15, marker='o', dic_opts={
        'padding': 0.1, 'tools': [''], 'xlabel': xlabel, 'ylabel': ylabel, 'clim': clim, 'alpha': 0.5, 'responsive': True, 'min_height':400}, groupby=groupby)

    
    # Plot scatter from data for all channels (rasterized)
    all_channels_scatter_plot = hvplot_df_scatter(l1_rate_data, x=x, y=y, title=title, color=y, cmap=cmap_custom,  size=20, marker='o', dic_opts={
                                                  'padding': 0.1, 'tools': [''], 'xlabel': xlabel, 'alpha': 0.15, 'ylabel': ylabel, 'clim': clim, 'responsive': True, 'min_height':400}, rasterize=True, dynamic=False)


    # Create a composite plot with all the plots merged
    composite_plot = lines_plot * single_channel_scatter_plot  * max_line_plot * all_channels_scatter_plot

    # L0 RATE CONTROL
    if l0_rate_control_data.empty is False:
        # Reset index of L0 Rate Control dataframe to be able to plot it
        l0_rate_control_data = l0_rate_control_data.reset_index()
   
        # Creates SPikes plot from L0 Rate Control dataframe
        max_value = df_with_min_max_avg['max'].max()
        l0_rate_control_plot = hv.Spikes(l0_rate_control_data.date, label='L0 Rate Control').opts(alpha=1, spike_length=max_value, line_width=2, line_color='orange', muted_alpha=0)

        composite_plot = composite_plot * l0_rate_control_plot

    # L1 RATE CONTROL
    if l1_rate_control_data.empty is False:
        l1_rate_control_data = l1_rate_control_data.reset_index()
        l1_rate_control_plot = hv.Spikes(l1_rate_control_data.date, label='L1 Rate Control').opts(alpha=1, spike_length=max_value, line_width=2, line_color='green', muted_alpha=0)

        composite_plot = composite_plot * l1_rate_control_plot

    # L1 RATE MAX
    if l1_rate_max_data.empty is False:
        # Select max value from L1 Rate Max dataframe
        l1_rate_max = l1_rate_max_data['l1_rate_max'].max()

        # Plot an horizontal blue dashed line with the max value
        #l1_rate_max_plot = hv.HLine(l1_rate_max, label='L1 Rate Max').opts(line_dash='dashed', line_width=1, line_color='red', muted_alpha=0)
        
        l1_rate_data = l1_rate_data.reset_index()
        min_date = l1_rate_data['date'].min()
        max_date = l1_rate_data['date'].max()
        inf_sim_min_date = l1_rate_data['date'].min() - pd.Timedelta(days=360)
        inf_sim_max_date = l1_rate_data['date'].max() + pd.Timedelta(days=360)
        
        orange_color = '#FF7F0E'
        l1_rate_max_plot = hv.Curve([(inf_sim_min_date, l1_rate_max), (inf_sim_max_date, l1_rate_max)], label='L1 Rate Max').opts(line_color=orange_color, line_width=2, muted_alpha=0, xlim=(min_date - pd.Timedelta(hours=1), max_date + pd.Timedelta(hours=1)))

        composite_plot = composite_plot * l1_rate_max_plot
   
   
    # L1 RATE TARGET
    if l1_rate_target_data.empty is False:
        # Select max value from L1 Rate Target dataframe
        l1_rate_target_data = l1_rate_target_data.reset_index()
        l1_rate_target_max = l1_rate_target_data['l1_rate_target'].max()
        
        # Create colorcet single color using hexadecimal
        cyan_color = '#17BECF'
        l1_rate_target_plot = hv.Curve([(inf_sim_min_date, l1_rate_target_max), (inf_sim_max_date, l1_rate_target_max)], label='L1 Rate Target').opts(line_color=cyan_color, line_width=2, muted_alpha=0, xlim=(min_date - pd.Timedelta(hours=1), max_date + pd.Timedelta(hours=1)))
        
        composite_plot = composite_plot * l1_rate_target_plot
    
    #composite_plot = lines_plot * single_channel_scatter_plot  * max_line_plot * all_channels_scatter_plot *  l1_rate_control_plot * l0_rate_control_plot * l1_rate_max_plot * l1_rate_target_plot

    return composite_plot.opts(legend_position='top', responsive=True, min_height=500, hooks=[disable_logo], show_grid=True, legend_opts={"click_policy": "hide"},)


def plot_l0_ipr_data(data_dict, x, y, title, xlabel, ylabel, groupby, cmap_custom, clim):
    """
    Plot L0 IPR data and L0 Rate Max data in a plot.

    The composite plot for L0 IPR data is composed by:
        - Lines plot with the max, min and average values for each date
        - Scatter plot with all the data for each channel
        - The scattered points corresponding with the values from the selected channel
        - The rasterized scattered points for the values from all channels



    Parameters
    ----------
    - `data_dict` (dict): The dictionary with the data to plot. It should contain the following keys: 
        - `l0_pixel_ipr`: Dataframe with the L0 IPR data
        - `l0_rate_max`: Dataframe with the L0 Rate Max data

    - `x` (str): Name of the column to be used as x axis
    - `y` (str): Name of the column to be used as y axis
    - `title` (str) : Title of the plot
    - `xlabel` (str): Label for the x axis
    - `ylabel` (str): Label for the y axis
    - `groupby` (str): Name of the column to be used to group the data (e.g. channel)
    - `cmap_custom` (LinearSegmentedColormap): Custom colormap to be used for the scatter plot
    - `clim` (tuple): Color limits for the scatter plot

    Returns
    -------
    - `composite_plot` (holoviews.core.overlay.Overlay): The composited plots created with hvPlot.
    """

    l0_pixel_ipr_data = data_dict['l0_pixel_ipr']
    l0_rate_max_data = data_dict['l0_rate_max']

    # Build a pandas dataframe from the original dataframe and select the min and max values for each date
    df_with_min_max_avg = build_min_max_avg(l0_pixel_ipr_data, x, y, groupby)
    
    # Just for debugging purposes: Check if we have duplicated indexes (dates) in the dataframe
    # print(df_with_min_max_avg[df_with_min_max_avg.index.duplicated(keep=False)])

    max_line_plot = hvplot_df_max_min_avg_line(df_with_min_max_avg, x=x, title=title, dic_opts={
        'padding': 0.1, 'tools': ['hover'], 'xlabel': xlabel, 'ylabel': ylabel, 'axiswise': True, 'show_legend': True}, category=groupby)
    
    # Plot lines grouped by channel from data (channel is selected by widget and just one channel is shown at a time)
    lines_plot = hvplot_df_grouped_line(l0_pixel_ipr_data, x=x, y=y, title=title, dic_opts={
        'padding': 0.1, 'tools': ['hover'], 'xlabel': xlabel, 'ylabel': ylabel, 'axiswise': True}, groupby=groupby, color='purple')

    # Plot scatter from data for a single channel
    single_channel_scatter_plot = hvplot_df_scatter(l0_pixel_ipr_data, x=x, y=y, title=title, color=y, cmap=cmap_custom, size=15, marker='o', dic_opts={
        'padding': 0.1, 'tools': [''], 'xlabel': xlabel, 'ylabel': ylabel, 'clim': clim, 'alpha': 0.5}, groupby=groupby)

    
    # Plot scatter from data for all channels (rasterized)
    all_channels_scatter_plot = hvplot_df_scatter(l0_pixel_ipr_data, x=x, y=y, title=title, color=y, cmap=cmap_custom,  size=20, marker='o', dic_opts={
                                                  'padding': 0.1, 'tools': [''], 'xlabel': xlabel, 'alpha': 0.15, 'ylabel': ylabel, 'clim': clim,}, rasterize=True, dynamic=False)

    l0_pixel_ipr_data = l0_pixel_ipr_data.reset_index()
    min_date = l0_pixel_ipr_data['date'].min()
    max_date = l0_pixel_ipr_data['date'].max()

    orange_color = '#FF7F0E'

    if (l0_rate_max_data.empty is False):
        # L0 RATE MAX
        # Select max value from L0 Rate Max dataframe
        l0_rate_max = l0_rate_max_data['l0_rate_max'].max()

        l0_rate_max_plot = hv.Curve([(min_date, l0_rate_max), (max_date, l0_rate_max)], label='L0 Rate Max').opts(line_color=orange_color, line_width=2, muted_alpha=0)

        # Create a composite plot with all the plots merged
        composite_plot = lines_plot * single_channel_scatter_plot  * max_line_plot * all_channels_scatter_plot * l0_rate_max_plot
    
    else:
        composite_plot = lines_plot * single_channel_scatter_plot  * max_line_plot * all_channels_scatter_plot
    
    return composite_plot.opts(legend_position='top', responsive=True, min_height=500, hooks=[disable_logo], show_grid=True, legend_opts={"click_policy": "hide"})


def plot_tib_rate_data(data_dict, title, xlabel, ylabel):
    """
    Plot the TIB Rates data
    
    Parameters
    ----------
    - `data_dict` (dict): The dictionary with the data to plot. It should contain the following keys:
        - `tib_busy_rate`: Dataframe with the TIB Busy Rate data
        - `tib_calibration_rate`: Dataframe with the TIB Calibration Rate data
        - `tib_camera_rate`: Dataframe with the TIB Camera Rate data
        - `tib_local_rate`: Dataframe with the TIB Local Rate data
        - `tib_pedestal_rate`: Dataframe with the TIB Pedestal Rate data
    - `title` (str): Title of the plot
    - `xlabel` (str): Label for the x axis
    - `ylabel` (str): Label for the y axis

    Returns
    -------
    - `composite_plot` (holoviews.core.overlay.Overlay): The composited plots created with hvPlot.
    """
    tib_busy_rate_data = data_dict['tib_busy_rate']
    tib_calibration_rate_data = data_dict['tib_calibration_rate']
    tib_camera_rate_data = data_dict['tib_camera_rate']
    tib_local_rate_data = data_dict['tib_local_rate']
    tib_pedestal_rate_data = data_dict['tib_pedestal_rate']
    
    tib_busy_rate_plot = tib_busy_rate_data.hvplot.line(x='date', y=tib_busy_rate_data, title=title, grid=True, responsive=True, min_height=400, legend='top', label='busy', muted_alpha=0, yformatter='%.0f')
    tib_calibration_rate_plot = tib_calibration_rate_data.hvplot.line(x='date', y=tib_calibration_rate_data, title=title, grid=True, responsive=True, min_height=400, label='calibration', legend='top', muted_alpha=0, yformatter='%.0f')
    tib_camera_rate_plot = tib_camera_rate_data.hvplot.line(x='date', y=tib_camera_rate_data, title=title, grid=True, responsive=True, min_height=400, legend='top', label='camera', muted_alpha=0, yformatter='%.0f')
    tib_local_rate_plot = tib_local_rate_data.hvplot.line(x='date', y=tib_local_rate_data, title=title, grid=True, responsive=True, min_height=400, legend='top', label='local', muted_alpha=0, yformatter='%.0f')
    tib_pedestal_rate_plot = tib_pedestal_rate_data.hvplot.line(x='date', y=tib_pedestal_rate_data, title=title, grid=True, responsive=True, min_height=400, label='pedestal', legend='top', muted_alpha=0, yformatter='%.0f')

    composite_plot = tib_busy_rate_plot * tib_calibration_rate_plot * tib_camera_rate_plot * tib_local_rate_plot * tib_pedestal_rate_plot
    return composite_plot.opts(legend_position='top', xlabel=xlabel, ylabel=ylabel, hooks=[disable_logo], show_grid=True, responsive=True, min_height=500, legend_opts={"click_policy": "hide"})


def plot_dragon_busy_data(data, title, xlabel, ylabel):
    """
    Plot the Dragon Busy data. It will create a scatter plot with the busy status of the modules.

    Parameters
    ----------
    - `data` (pandas.core.frame.DataFrame): The dataframe with the data to plot
    - `title` (str): Title of the plot
    - `xlabel` (str): Label for the x axis
    - `ylabel` (str): Label for the y axis
    """
    busy_plot = data.hvplot.scatter(x='date', y='module', c='busy_status', cmap='viridis', title=title, grid=True, responsive=True, min_height=400, max_height=750)
    return busy_plot.opts(xlabel=xlabel, ylabel=ylabel, hooks=[disable_logo], show_grid=True, responsive=True, min_height=500, max_height=750, clim=(0, 3), color_levels=[0, 0.5, 1.5, 2.5, 3], clabel='Busy Status', colorbar_opts={'title_standoff': -150, 'padding': 30})
    