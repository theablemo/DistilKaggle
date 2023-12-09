import os
import json
import pandas as pd
from tqdm import tqdm

# Function to save the dataframes in a file
def save_file(last_folder_batch, code_batches_folder_path, markdown_batches_folder_path):
    print('#################### saving file #####################')
    bach_counter = 'batch_' + str(int(last_folder_batch)) + '.csv'
    df_codes.to_csv(f'{code_batches_folder_path}/code_' + bach_counter, index=False)
    df_markdown.to_csv(f'{markdown_batches_folder_path}/markdown_' + bach_counter, index=False)

# Set paths
log_file_path = '../Log.csv' # Log file path
dataframes_folder_path = '../dataframes' # Dataframes path
code_batches_folder_path = f'{dataframes_folder_path}/code_batches' # Code batches path
markdown_batches_folder_path = f'{dataframes_folder_path}/markdown_batches' # Markdown batches path

# for illustration dataframes in output in a better way and showing all columns and rows you want
desired_width = 320
pd.set_option('display.width', desired_width)
pd.set_option('display.max_columns', 10)


# Concatenate all str variables in a list and create a final long string
def list_to_str(ls):
    tmp = ''
    for x in ls:
        tmp += x
    return tmp

# Check if the folders exist
if not os.path.exists(dataframes_folder_path):
    os.makedirs(dataframes_folder_path)
if not os.path.exists(code_batches_folder_path):
    os.makedirs(code_batches_folder_path)
if not os.path.exists(markdown_batches_folder_path):
    os.makedirs(markdown_batches_folder_path)

# define dataframes for markdown and codes separately
df_codes = pd.DataFrame(
    columns=['kernel_id', 'current_kernel_version_id', 'cell_index', 'source', 'output_type', 'execution_count'])
df_markdown = pd.DataFrame(columns=['kernel_id', 'current_kernel_version_id', 'cell_index', 'source'])
print(df_markdown)
print(df_codes)

# except list is a dataframe of filenames which had error in any part of their information extraction
# this df consist of two column which contains filename and a brief description of error
# ex: df_exceptions.ilco[1] = ['filename1', 'while opening file']
df_exceptions = pd.DataFrame(columns=['kernel_id', 'current_kernel_version_id', 'section'])

# Read the CSV file into a DataFrame
log_df = pd.read_csv(log_file_path)

# Initialize folder batch changed
folder_batch_changed = False

# read files in a loop and extract their information
for index, row in tqdm(log_df.iterrows(), total=len(log_df)):
    
    # Check if the download status is ok
    if row['DownloadStatus'] != 'ok':
        continue
    
    # Initialize last folder batch in the first iteration
    if index == 0:
        last_folder_batch = str(row['FolderBatch'])
        
    # Initialize kernel info
    current_kernel_version_id = str(row['CurrentKernelVersionId'])
    user_id = str(row['UserId'])
    kernel_id = str(row['KernelId'])
    folder_batch = str(row['FolderBatch'])
    
    # Check if folder batch is changed
    if folder_batch != last_folder_batch:
        # Save the dataframes in a file
        save_file(last_folder_batch, code_batches_folder_path, markdown_batches_folder_path)
        # Reset the dataframes
        df_codes = pd.DataFrame(
        columns=['kernel_id', 'current_kernel_version_id', 'cell_index', 'source', 'output_type', 'execution_count'])
        df_markdown = pd.DataFrame(columns=['kernel_id', 'current_kernel_version_id', 'cell_index', 'source'])
    
    # Update last folder batch
    last_folder_batch = folder_batch
        
    # Set file path
    file_path = f'../{folder_batch}/{current_kernel_version_id}_{user_id}_{kernel_id}.ipynb'
    # print("file to be analyzed: " + file_path)
    
    # Opening file
    # 2
    try:
        f = open(file_path, encoding="utf8")
        data = json.load(f)
    except:
        df_tmp = pd.DataFrame(
                    {'kernel_id': kernel_id, 'current_kernel_version_id': current_kernel_version_id, 'section': 'opening file'}, index=[0])
        df_exceptions = pd.concat([df_exceptions, df_tmp], ignore_index=True, axis=0)
        continue

    # Going through each cell in the file
    # print("Going through its cells...")
    cnt_cell = 0

    try:
        cells = data['cells']
    except:
        continue
    
    for cell in cells:
        cnt_cell += 1
        # 3
        try:
            string_src = cell['source']
        except:
            df_tmp = pd.DataFrame(
                {'kernel_id': kernel_id, 'current_kernel_version_id': current_kernel_version_id, 'section': 'cell[\'source\']'}, index=[0])
            df_exceptions = pd.concat([df_exceptions, df_tmp], ignore_index=True, axis=0)
            continue

        if type(string_src) is list:
            string_src = list_to_str(string_src)

        if cell['cell_type'] == 'code':
            try:
                output_type = cell['outputs'][0]['output_type']
            except:
                output_type = None

            # 4
            try:
                df_tmp = pd.DataFrame(
                    {'kernel_id': kernel_id, 'current_kernel_version_id': current_kernel_version_id, 'cell_index': cnt_cell, \
                     'source': string_src, 'output_type': output_type, 'execution_count': cell['execution_count']}, index=[0])
                df_codes = pd.concat([df_codes, df_tmp], ignore_index=True, axis=0)
            except:
                df_tmp = pd.DataFrame(
                    {'kernel_id': kernel_id, 'current_kernel_version_id': current_kernel_version_id, 'section': 'appending code cell'}, index=[0])
                df_exceptions = pd.concat([df_exceptions, df_tmp], ignore_index=True, axis=0)

        elif cell['cell_type'] == 'markdown':
            try:
                df_tmp = pd.DataFrame(
                    {'kernel_id': kernel_id, 'current_kernel_version_id': current_kernel_version_id, 'cell_index': cnt_cell, 'source': string_src},
                    index=[0])
                df_markdown = pd.concat([df_markdown, df_tmp], ignore_index=True, axis=0)
            except:
                df_tmp = pd.DataFrame(
                    {'kernel_id': kernel_id, 'current_kernel_version_id': current_kernel_version_id, 'section': 'appending markdown cell'}, index=[0])
                df_exceptions = pd.concat([df_exceptions, df_tmp], ignore_index=True, axis=0)
        else:
            f.close()
            continue
    
    # Save the dataframes in a file
    if index == len(log_df) - 1:
        save_file(last_folder_batch, code_batches_folder_path, markdown_batches_folder_path)
    
    f.close()

df_exceptions.to_csv(f'{dataframes_folder_path}/exception_files.csv', index=False)