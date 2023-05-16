import pandas as pd
import holoviews as hv # noqa
import gc

gc.enable()

def hvplot_df_line(df:pd.DataFrame, x, y, title:str, dic_opts:dict, groupby:str, color:str='green'):
    """
    Plots a pandas dataframe line plot.
    """
    
    dynamic_map = df.hvplot.line(x=x, y=y, title=title, color=color, groupby=groupby, hover_cols='all',
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
    
    max_line = df.hvplot.line(x=x, y='max', title=title, responsive=True, min_height=400, hover_cols=['x', 'y', 'max_' + category], label='max', color='red').opts(alpha=1, muted_alpha=0)
        
    min_line = df.hvplot.line(x=x, y='min', title=title, responsive=True, min_height=400, hover_cols=['x', 'y', 'min_' + category], label='min', color='blue').opts(alpha=1, muted_alpha=0)
    
    mean_line = df.hvplot.line(x=x, y='avg', title=title, responsive=True, min_height=400, hover_cols=['x', 'y'], label='avg', color='black').opts(alpha=1, muted_alpha=0)
    
    options = list(dic_opts.items())
    
    dynamic_map = max_line * mean_line * min_line
    dynamic_map.opts(**dict(options))
    dynamic_map
    

    return dynamic_map



def hvplot_df_scatter(df, x, y, title, color, size, marker, dic_opts, cmap="reds", groupby=None, datashade=False, rasterize=False, dynamic=True):
    """_summary_
    Plot a scatter graph from a pandas dataframe
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
    def process_chunk(df_chunk, x, y, category):
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
    Disables blokeh logo in plots
    """
    plot.state.toolbar.logo = None



def plot_data(data, x, y, title, xlabel, ylabel, groupby, cmap_custom, clim):

    # Build a pandas dataframe from the original dataframe and select the min and max values for each date
    df_with_min_max_avg = build_min_max_avg(data, x, y, groupby)
    
    # Just for debugging purposes: Check if we have duplicated indexes (dates) in the dataframe
    # print(df_with_min_max_avg[df_with_min_max_avg.index.duplicated(keep=False)])

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
                                                  'padding': 0.1, 'tools': [''], 'xlabel': xlabel, 'alpha': 0.15, 'ylabel': ylabel, 'clim': clim,}, rasterize=True, dynamic=False)

    # Create a composite plot with all the plots merged
    composite_plot = lines_plot * single_channel_scatter_plot  * max_line_plot * all_channels_scatter_plot

    return composite_plot.opts(legend_position='top', responsive=True, min_height=500, hooks=[disable_logo], show_grid=True)



def plot_l1_rate_data(data_list, x, y, title, xlabel, ylabel, groupby, cmap_custom, clim):

    data = data_list[0]
    l0_r_control = data_list[1]
    l1_r_control = data_list[2]
    l1_rate_max_data = data_list[3]

    # Build a pandas dataframe from the original dataframe and select the min and max values for each date
    df_with_min_max_avg = build_min_max_avg(data, x, y, groupby)
    
    # Just for debugging purposes: Check if we have duplicated indexes (dates) in the dataframe
    # print(df_with_min_max_avg[df_with_min_max_avg.index.duplicated(keep=False)])

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
                                                  'padding': 0.1, 'tools': [''], 'xlabel': xlabel, 'alpha': 0.15, 'ylabel': ylabel, 'clim': clim,}, rasterize=True, dynamic=False)

    # L0 RATE CONTROL
    # Reset index of L0 Rate Control dataframe to be able to plot it
    l0_r_control = l0_r_control.reset_index()
   
    # Creates SPikes plot from L0 Rate Control dataframe
    max_value = df_with_min_max_avg['max'].max()
    l0_r_control_plot = hv.Spikes(l0_r_control.date, label='L0 Rate Control').opts(alpha=1, spike_length=max_value, line_width=2, line_color='orange', muted_alpha=0)

    # L1 RATE CONTROL
    l1_r_control = l1_r_control.reset_index()
    l1_r_control_plot = hv.Spikes(l1_r_control.date, label='L1 Rate Control').opts(alpha=1, spike_length=max_value, line_width=2, line_color='green', muted_alpha=0)

    # L1 RATE MAX
    # Select max value from L1 Rate Max dataframe
    l1_rate_max = l1_rate_max_data['l1_rate_max'].max()

    # Plot an horizontal blue dashed line with the max value
    #l1_rate_max_plot = hv.HLine(l1_rate_max, label='L1 Rate Max').opts(line_dash='dashed', line_width=1, line_color='red', muted_alpha=0)

    # create a horizontal line using hv.curve. In the x axis the first point is the min datetime and in the y axis is the l1_rate_max value, for the second point the x axis is the max datetime and the y axis is the l1_rate_max value

    data = data.reset_index()
    min_date = data['date'].min()
    max_date = data['date'].max()

    # build a dataframe with the min and max dates and the l1_rate_max value
    l1_rate_max_df = pd.DataFrame({'date': [min_date, max_date], 'l1_rate_max': [l1_rate_max, l1_rate_max]})
    
    l1_rate_max_plot = hv.Curve([(min_date, l1_rate_max), (max_date, l1_rate_max)], label='L1 Rate Max').opts(line_color='pink', line_width=2, muted_alpha=0)
    #l1_rate_max_plot = l1_rate_max_df.hvplot.line(x='date', y='l1_rate_max', label='L1 Rate Max').opts(line_dash='dashed', line_width=2, line_color='red', muted_alpha=0)
    #l1_rate_max_plot = hv.HLine(l1_rate_max, label='L1 Rate Max').opts(line_dash='dashed', line_width=1, line_color='red', muted_alpha=0)
    
    # Create a composite plot with all the plots merged
    composite_plot = lines_plot * single_channel_scatter_plot  * max_line_plot * all_channels_scatter_plot *  l1_r_control_plot * l0_r_control_plot * l1_rate_max_plot

    return composite_plot.opts(legend_position='top', responsive=True, min_height=500, hooks=[disable_logo], show_grid=True)