### NOTE: `1_dataframe_generator.py` should be ran prior to running this file


import os
import pandas as pd

# Set paths
log_file_path = '../Log.csv' # Log file path
dataframes_folder_path = '../dataframes' # Dataframes path
code_batches_folder_path = f'{dataframes_folder_path}/code_batches' # Code batches path
markdown_batches_folder_path = f'{dataframes_folder_path}/markdown_batches' # Markdown batches path
code_dataframe = f'{dataframes_folder_path}/code.csv'
markdown_dataframe = f'{dataframes_folder_path}/markdown.csv'

def concatenate_csv_files(input_dir, output_file):
    # Get a list of all CSV files in the input directory
    csv_files = [file for file in os.listdir(input_dir) if file.endswith('.csv')]

    # Check if there are any CSV files
    if not csv_files:
        print("No CSV files found in the specified directory.")
        return

    # Read and concatenate the CSV files
    dfs = [pd.read_csv(os.path.join(input_dir, file)) for file in csv_files]
    concatenated_df = pd.concat(dfs, ignore_index=True)

    # Write the concatenated DataFrame to a new CSV file
    concatenated_df.to_csv(output_file, index=False)
    print(f"Concatenated data saved to {output_file}")


# Concatenate code
concatenate_csv_files(code_batches_folder_path, code_dataframe)

# Concatenate markdown
concatenate_csv_files(markdown_batches_folder_path, markdown_dataframe)
