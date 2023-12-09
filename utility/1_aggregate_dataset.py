import pandas as pd
import os
from KG_adapter import adapt_adapter

# Set paths
dataset_directory = '../dataset' # Dataframes path
code_dataframe = f'{dataset_directory}/code.csv'
markdown_dataframe = f'{dataset_directory}/markdown.csv'
notebook_metrics_dataframe = f'{dataset_directory}/notebook_metrics.csv'

sources = [5, 4, 3, 2, 1, 0, 'KGT']

# Check if dataset directory exists
if not os.path.exists(dataset_directory):
    os.makedirs(dataset_directory)

def concatenate_csv_files(csv_files, output_file):
    # Initialize an empty DataFrame to store the concatenated data
    concatenated_df = pd.DataFrame()

    # Iterate through each CSV file and concatenate to the DataFrame
    for csv_file in csv_files:
        if 'KGT' in csv_file:
            df = pd.read_csv(csv_file, index_col=0, dtype={"kernel_id": int})
        else:
            df = pd.read_csv(csv_file)
        concatenated_df = pd.concat([concatenated_df, df], ignore_index=True)

    # Write the concatenated DataFrame to a new CSV file
    concatenated_df.to_csv(output_file, index=False)
    print(f"Concatenation complete. Result saved to {output_file}")


def concatenate_csv_files2(csv_files, output_file):
    first_one = True
    CHUNK_SIZE = 50000
    for csv_file_name in csv_files:
        if not first_one: # if it is not the first csv file then skip the header row (row 0) of that file
            skip_row = [0]
        else:
            skip_row = []

        chunk_container = pd.read_csv(csv_file_name, chunksize=CHUNK_SIZE, skiprows = skip_row)
        for chunk in chunk_container:
            chunk.to_csv(output_file, mode="a", index=False)
        first_one = False

# Get a list of all CSV files in the specified path
code_csv_files = [f'../{x}/dataframes/code.csv' for x in sources]
markdown_csv_files = [f'../{x}/dataframes/markdown.csv' for x in sources]
notebook_metrics_csv_files = [f'../{x}/metrics/notebook_metrics.csv' for x in sources]

# Adapt dataframes (IF NEEDED)
# adapt_adapter(code_csv_files, markdown_csv_files, notebook_metrics_csv_files)

# Call the function to concatenate CSV files
# concatenate_csv_files2(code_csv_files, code_dataframe)
concatenate_csv_files2(markdown_csv_files, markdown_dataframe)
# concatenate_csv_files2(notebook_metrics_csv_files, notebook_metrics_dataframe)
