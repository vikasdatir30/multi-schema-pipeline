import os
import datetime

import pandas as pd


def create_temp_dir(folder_path):
    try:
        # Create the folder
        os.mkdir(folder_path)
        print(f'Folder "{folder_path}" created successfully')
    except FileExistsError:
        print(f'Folder "{folder_path}" already exists')


def list_files_with_parent_dirs(directory):
    for root, _, files in os.walk(directory):
        for file in files:
            print(file)
            file_path = os.path.join(root, file)
            print(file_path)

