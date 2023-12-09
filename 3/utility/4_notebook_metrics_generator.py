### NOTE: `3_aggregated_dataframe_generator.py` should be ran prior to running this file
import pandas as pd
from tqdm import tqdm
tqdm.pandas()

# PT
pt = 3

# Set paths
metrics_folder_path = '../metrics'
code_cell_metrics_dataframe = f'{metrics_folder_path}/code_cell_metrics.csv'
markdown_cell_metrics_dataframe = f'{metrics_folder_path}/markdown_cell_metrics.csv'
notebook_metrics_dataframe = f'{metrics_folder_path}/notebook_metrics.csv'

# Get dataframes
code_df = pd.read_csv(code_cell_metrics_dataframe)
md_df = pd.read_csv(markdown_cell_metrics_dataframe)

# Aggregate code metrics
pm1 = code_df[['kernel_id','LOC', 'BLC', 'UDF', 'I', 'EH','NVD','NEC','S','P'
               ,'OPRND','AOPERATOR','LOPERATOR','UOPRND','UOPRATOR','ID','LOCom', 'EAP',
               'CW']].groupby(by='kernel_id').sum().reset_index()
pm2 =  code_df[['kernel_id','ALID','LOC','ALLC','KLCID','CyC']].groupby(by='kernel_id').mean().reset_index()
pm2.rename(columns={'LOC': 'MeanLCC'}, inplace= True)
pm3 =  code_df[['kernel_id','MLID','NBD','ID']].groupby(by='kernel_id').max().reset_index().rename(columns={'ID':'AID'})
pm4 = code_df[['kernel_id', 'LOC']].groupby('kernel_id').count().reset_index().rename(columns={'LOC':'CC'})
notebook_cell1 = pd.concat([pm1, pm2.drop(columns=['kernel_id']), 
                            pm3.drop(columns=['kernel_id']), pm4.drop(columns=['kernel_id'])], axis=1)

# Aggregate markdown metrics
qm1 = md_df[['kernel_id', 'H1']].groupby('kernel_id').count().reset_index().rename(columns={'H1': 'MC'})
qm2 = md_df[['kernel_id', 'MW', 'LMC']].groupby('kernel_id').mean().reset_index().rename(columns={'MW':'MeanWMC', 'LMC':'MeanLMC'})
qm3 = md_df[['kernel_id', 'H1', 'H2', 'H3', 'MW', 'LMC']].groupby('kernel_id').sum().reset_index()
notebook_cell2 = pd.concat([qm1, qm2.drop(columns=['kernel_id']), qm3.drop(columns=['kernel_id'])], axis=1)

# Some validations
notebook_cell1 = notebook_cell1[notebook_cell1.kernel_id != 'train_emb']
notebook_cell2 = notebook_cell2[notebook_cell2.kernel_id != 'train_emb']

notebook_cell1['kernel_id'] = pd.to_numeric(notebook_cell1['kernel_id'], errors='coerce')
notebook_cell2['kernel_id'] = pd.to_numeric(notebook_cell2['kernel_id'], errors='coerce')
notebook_cell1 = notebook_cell1.dropna(subset=['kernel_id'])
notebook_cell2 = notebook_cell2.dropna(subset=['kernel_id'])

notebook_cell1.kernel_id =notebook_cell1['kernel_id'].astype(int)
notebook_cell2.kernel_id =notebook_cell2['kernel_id'].astype(int)
notebook_cell1 = notebook_cell1[notebook_cell1['LOC'] > 1]

# Merge markdown and cell metrics
notebook_cell = notebook_cell1.merge(notebook_cell2, how='left')

# Add PT
notebook_cell['PT'] = pt

# Save notebook metrics
print("###########SAVING NOTEBOOK METRICS...###########")
notebook_cell.to_csv(notebook_metrics_dataframe, index=False)
