# DistilKaggle: A Distilled Dataset of Kaggle Jupyter Notebooks

Access the dataset: [![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.10317389.svg)](https://doi.org/10.5281/zenodo.10317389)

## Overview

DistilKaggle is a curated dataset extracted from Kaggle Jupyter notebooks spanning from September 2015 to October 2023. This dataset is a distilled version derived from the download of over 300GB of Kaggle kernels, focusing on essential data for research purposes. The dataset exclusively comprises publicly available Python Jupyter notebooks from Kaggle. The essential information for retrieving the data needed to download the dataset is obtained from the MetaKaggle dataset provided by Kaggle
## Contents

The DistilKaggle dataset consists of three main CSV files:

1. **code.csv**: Contains over 12 million rows of code cells extracted from the Kaggle kernels. Each row is identified by the kernel's ID and cell index for reproducibility.

2. **markdown.csv**: Includes over 5 million rows of markdown cells extracted from Kaggle kernels. Similar to code.csv, each row is identified by the kernel's ID and cell index.

3. **notebook_metrics.csv**: This file provides notebook features described in the accompanying paper released with this dataset. It includes metrics for over 517,000 Python notebooks.

## Directory Structure

The kernels directory is organized based on Kaggle's Performance Tiers (PTs), a ranking system in Kaggle that classifies users. The structure includes PT-specific directories, each containing user ids that belong to this PT, download logs, and the essential data needed for downloading the notebooks.

The utility directory contains two important files:

1. **aggregate_data.py**: A Python script for aggregating data from different PTs into the mentioned CSV files.

2. **application.ipynb**: A Jupyter notebook serving as a simple example application using the metrics dataframe. It demonstrates predicting the PT of the author based on notebook metrics.

## Usage

Researchers can leverage this distilled dataset for various analyses without dealing with the bulk of the original 300GB dataset. For access to the raw, unprocessed Kaggle kernels, researchers can request the dataset directly.

## Note

The original dataset of Kaggle kernels is substantial, exceeding 300GB, making it impractical for direct upload to Zenodo. Researchers interested in the full dataset can contact the dataset maintainers for access.

## Citation

If you use this dataset in your research, please cite the accompanying paper or provide appropriate acknowledgment as outlined in the documentation.

Thank you for using DistilKaggle!
