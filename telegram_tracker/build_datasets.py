# -*- coding: utf-8 -*-

# import modules
import pandas as pd
import numpy as np
import argparse
import msgspec
import h5py
import json
import glob
import time
import csv
import json
import glob
import time
import os
import pdb
#import pyarrow.parquet as pq 
import pickle
# import submodules
from tqdm import tqdm


# local submodules
from .constructors.constructors import TGMessages, TGResponse
from .utils.processor import processor

# local definitions
from .utils.definitions import (
    chats_dataset_columns, clean_msg, msg_attrs, get_forward_attrs, get_reply_attrs,
    get_url_attrs, get_document_attrs, get_poll_attrs, get_contact_attrs,
    get_geo_attrs, msgs_dataset_columns
)


def main():


    '''

    Arguments

    '''

    parser = argparse.ArgumentParser(description='Arguments.')
    parser.add_argument(
            '--input-path',
            '-',
            type=str,
            required=False,
        help='Path where existing data from telegram-tracker is located. Will use `./output/data` if not given.'
        )

    parser.add_argument(
            '--type',
            '-t',
            type=str,
            required=True,
            help='Data output type'
            )

    parser.add_argument(
            '--output-path',
            '-0',
            type=str,
            required=True,
            help='Data output location')


    # Parse arguments
    args = vars(parser.parse_args())

    # get main path
    if args['input_path']:
        main_path = args['input_path']
        if main_path.endswith('/'):
            main_path = main_path[:-1]
    else:
        main_path = './output/data'

    # log results
    text = f'''
    Init program at {time.ctime()}

    '''
    print(text)



    # Path Settings:

    # Collect JSON files
    if not args['input_path']:
        
        json_files_path = f'{main_path}/**/*_messages.json'
        json_files = glob.glob(
                os.path.join(json_files_path),
                recursive=True
                )

    else:

        # check all path case instances:
        if args['input_path'].endswith != "_messages.json":

            holder = args['input_path']
            json_files = glob.glob(
                os.path.join(holder),
                recursive=True
                )
        else:

            json_files_path = args['input_path']
            json_files = glob.glob(
                    os.path.join(json_files_path),
                    recursive=True
                    )

            if len(json_files_path) == 0:

                try:

                    holder = args['input_path']
                    json_files_path = f'{holder}_messages.json'

                except Exception as e:
                    print(e)
                    pass

    # Save dataset
    msgs_file_path = f'{main_path}/msgs_dataset.csv'
    msgs_data_columns = msgs_dataset_columns()

    for json_file in json_files:
        
        # Do full processing:
        # Init values
        username = json_file.split('.json')[0].replace('\\', '/').split('/')[-1].replace(
        '_messages', '')

        channel_name = username

        #chats_file_path = f'{main_path}/{channel_name}/{channel_name}_metadata.csv'

        print("\n")

        # contains tuple of dict and pd.DataFrame (metadata object)

        full_obj = processor(json_file)

        # list comprehension to grab all the messages:
        data_list = [msg['bulk_channel'] for msg in full_obj[0]['msg']]

        # metadata
        metadata = full_obj[1]


        ######## FIX NEXT:
        # working, it just be tidied up and saved in a data efficient format
        #if args['type'] == '.csv':

        # data output path set with file extension:
        if not args['output_path']:
    
            channel_path = main_path +  channel_name + "_flat_messages" + args['type']
            meta_path = main_path + channel_name + "_metadata" + args['type']

        
        else:

            channel_path = args['output_path'] + channel_name + "_flat_messages" + args['type']
            meta_path = args['output_path'] + channel_name + "_metadata" + args['type']

            # write specific data structures form args['type']
            # csv

            if args['type'] == '.csv':

                df = pd.DataFrame(data_list)
                df.to_csv(channel_path, index=False)
                metadata.to_csv(meta_path, index=False)

            # pkl
            if args['type'] == '.pkl':

                with open(channel_path, 'wb') as file:
                    pickle.dump(data_list, file)
                    metadata.to_pickle(meta_path)

            else:

                continue
        #if args['type'] == '.parquet':
        #    df.to_parquet(channel_path, index=False)
                    

        # if output = .hdf5
        #if args['type'] == '.hdf5':

        #    try:
                    
        #        with h5py.File('data.hdf5', 'w') as file:
                        
        #            full = np.array(full_messages['msg']['0']['bulk_channel'])

        #            file.create_dataset(full)

        #    except Exception as e:
        #        print(e)


        #except FileNotFoundError:
        #    print("Incomplete channel directory - skipping.")
        #    pass

        #except Exception as e:
        #    print("Other unexpected exception - skipping.")
        #    pass
    



    ######

    #data.to_excel(
    #    chats_file_path.replace('.csv', '.xlsx'),
    #    index=False
    #            )

    # Save data
    #chats_columns = chats_dataset_columns()
    #data = data[chats_columns].copy()

if __name__ == "__main__":
    main()
