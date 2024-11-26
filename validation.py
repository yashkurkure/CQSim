import pandas as pd
import os
import plotly.graph_objects as go
import plotly.figure_factory as ff

swf_columns = [
    'id',             #1
    'submit',         #2
    'wait',           #3
    'run',            #4
    'used_proc',      #5
    'used_ave_cpu',   #6
    'used_mem',       #7
    'req_proc',       #8
    'req_time',       #9
    'req_mem',        #10 
    'status',         #11
    'user_id',        #12
    'group_id',       #13
    'num_exe',        #14
    'num_queue',      #15
    'num_part',       #16
    'num_pre',        #17
    'think_time',     #18
    ]

rst_columns = ['id', 'proc1', 'proc2','walltime', 'run', 'wait', 'submit', 'start', 'end']



def read_swf(path):
    """
    Reads an SWF file into a dataframe

    Args:
        filename: The name of the file to read.

    Returns:
        A list of lists, where each inner list represents a processed line of input.
    """
    data = []

    with open(f'{path}', 'r') as file:
        for line in file:
        
            # TODO: For now ignoring the header of the swf file
            if line[0] == ';':
                continue

            # Split the line into elements, convert non-empty elements to integers
            row = [int(x) for x in line.split() if x]
            data.append(row)
    df = pd.DataFrame(data, columns=swf_columns)
    return df

def read_rst(path):
    column_names = rst_columns
    df = pd.read_csv(f'{path}', sep=';', header=None) 
    df.columns = column_names
    df_sorted = df.sort_values(by='id')
    return df_sorted


def validation_plot_wait_delta_histogram(original_swf_file_path, cqsim_result_file_path, fig_name):

    actual_waits = read_swf(original_swf_file_path).sort_values(by='id')
    calculated_waits = read_rst(cqsim_result_file_path).sort_values(by='id')

    # Reset index to ensure alignment
    actual_waits = actual_waits.reset_index(drop=True)
    calculated_waits = calculated_waits.reset_index(drop=True)

    # Ensure both dataframes have the same number of rows and corresponding IDs
    if len(actual_waits) != len(calculated_waits):
        raise ValueError("DataFrames have different lengths!")
    if not (actual_waits['id'] == calculated_waits['id']).all():
        raise ValueError("DataFrames have mismatched IDs!")

    # Calculate the difference in 'wait' column (assumed to be in seconds)
    wait_delta = actual_waits['wait'] - calculated_waits['wait']

    # Convert wait_delta to hours
    wait_delta_hours = wait_delta / 3600 

    # Create a new DataFrame
    delta_df = pd.DataFrame({'id': actual_waits['id'], 'wait_delta_hours': wait_delta_hours})

    fig = go.Figure(data=[go.Histogram(x=delta_df['wait_delta_hours'], nbinsx=100)])

    # Set labels and title
    fig.update_layout(
        xaxis_title="Wait Time Delta (hours)",
        yaxis_title="Job Count",
        title="Distribution of Wait Time Deltas"
    )

    # Save the plot as a PNG file
    fig.write_image(fig_name)

def validation_plot_wait_delta_distplot(original_swf_file_path, cqsim_result_file_path, fig_name):

    actual_waits = read_swf(original_swf_file_path).sort_values(by='id')
    calculated_waits = read_rst(cqsim_result_file_path).sort_values(by='id')

    # Reset index to ensure alignment
    actual_waits = actual_waits.reset_index(drop=True)
    calculated_waits = calculated_waits.reset_index(drop=True)

    # Ensure both dataframes have the same number of rows and corresponding IDs
    if len(actual_waits) != len(calculated_waits):
        raise ValueError("DataFrames have different lengths!")
    if not (actual_waits['id'] == calculated_waits['id']).all():
        raise ValueError("DataFrames have mismatched IDs!")

    # Calculate the difference in 'wait' column (assumed to be in seconds)
    wait_delta = actual_waits['wait'] - calculated_waits['wait']

    # Convert wait_delta to hours
    wait_delta_hours = wait_delta / 3600 

    # Create a new DataFrame
    delta_df = pd.DataFrame({'id': actual_waits['id'], 'wait_delta_hours': wait_delta_hours})

    # Create the KDE plot
    fig = ff.create_distplot([delta_df['wait_delta_hours']], ['Wait Time Delta'], show_hist=False)

    # Set labels and title
    fig.update_layout(
        xaxis_title="Wait Time Delta (hours)",
        yaxis_title="Density",
        title="Distribution of Wait Time Deltas"
    )

    # Save the plot as a PNG file
    fig.write_image(fig_name)

def validation_plot_wait_delta_histogram(original_swf_file_path, cqsim_result_file_path, fig_name):

    actual_waits = read_swf(original_swf_file_path).sort_values(by='id')
    calculated_waits = read_rst(cqsim_result_file_path).sort_values(by='id')

    # Reset index to ensure alignment
    actual_waits = actual_waits.reset_index(drop=True)
    calculated_waits = calculated_waits.reset_index(drop=True)

    # Ensure both dataframes have the same number of rows and corresponding IDs
    if len(actual_waits) != len(calculated_waits):
        raise ValueError("DataFrames have different lengths!")
    if not (actual_waits['id'] == calculated_waits['id']).all():
        raise ValueError("DataFrames have mismatched IDs!")

    # Calculate the difference in 'wait' column (assumed to be in seconds)
    wait_delta = actual_waits['wait'] - calculated_waits['wait']

    # Convert wait_delta to hours
    wait_delta_hours = wait_delta / 3600 

    # Create a new DataFrame
    delta_df = pd.DataFrame({'id': actual_waits['id'], 'wait_delta_hours': wait_delta_hours})

    fig = go.Figure(data=[go.Histogram(x=delta_df['wait_delta_hours'], nbinsx=10)])

    # Set labels and title
    fig.update_layout(
        xaxis_title="Wait Time Delta (hours)",
        yaxis_title="Job Count",
        title="Distribution of Wait Time Deltas"
    )

    # Save the plot as a PNG file
    fig.write_image(fig_name)

def validation_plot_wait_delta_lineplot(original_swf_file_path, cqsim_result_file_path, fig_name):

    actual_waits = read_swf(original_swf_file_path).sort_values(by='id')
    calculated_waits = read_rst(cqsim_result_file_path).sort_values(by='id')

    # Reset index to ensure alignment
    actual_waits = actual_waits.reset_index(drop=True)
    calculated_waits = calculated_waits.reset_index(drop=True)

    # Ensure both dataframes have the same number of rows and corresponding IDs
    if len(actual_waits) != len(calculated_waits):
        raise ValueError("DataFrames have different lengths!")
    if not (actual_waits['id'] == calculated_waits['id']).all():
        raise ValueError("DataFrames have mismatched IDs!")

    # Calculate the difference in 'wait' column (assumed to be in seconds)
    wait_delta = actual_waits['wait'] - calculated_waits['wait']

    # Convert wait_delta to hours
    wait_delta_hours = wait_delta / 3600 

    # Create a new DataFrame
    delta_df = pd.DataFrame({'id': actual_waits['id'], 'wait_delta_hours': wait_delta_hours})

    # Create the KDE plot
    fig = go.Figure(data=[go.Scatter(x=delta_df['id'], y=delta_df['wait_delta_hours'], mode='markers')])

    # Set labels and title
    fig.update_layout(
        xaxis_title="Job ID",
        yaxis_title="Wait Time Delta",
        title="Wait Time Deltas per job"
    )

    # Save the plot as a PNG file
    fig.write_image(fig_name)


validation_plot_wait_delta_histogram(
    original_swf_file_path='data/InputFiles/pbsstream.swf',
    cqsim_result_file_path='data/Results/pbsstream.rst',
    fig_name='hist_bf1_e_reqTime2.png'
)

validation_plot_wait_delta_distplot(
    original_swf_file_path='data/InputFiles/pbsstream.swf',
    cqsim_result_file_path='data/Results/pbsstream.rst',
    fig_name='dist_bf1_e_reqTime2.png'
)

validation_plot_wait_delta_lineplot(
    original_swf_file_path='data/InputFiles/pbsstream.swf',
    cqsim_result_file_path='data/Results/pbsstream.rst',
    fig_name='line_bf1_e_reqTime2.png'
)