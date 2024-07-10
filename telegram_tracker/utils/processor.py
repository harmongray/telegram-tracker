import os
import sys
from ..constructors import * 
import h5py
import json
import pandas as pd
import pdb
from typing import Tuple, Dict
from tqdm import tqdm 
import msgspec

from ..constructors.constructors import TGMessages, TGResponse
from .definitions import *

# local definitions
from .definitions import (
        chats_dataset_columns, clean_msg, msg_attrs, get_forward_attrs, get_reply_attrs,
        get_url_attrs, get_document_attrs, get_poll_attrs, get_contact_attrs, get_geo_attrs, msgs_dataset_columns,
        get_channel_name
                    )


def processor(json_file) -> Tuple[Dict, pd.DataFrame]:
    # JSON files

    '''

    Iterate JSON files
    '''
    #  Get channel name
    channel_name = json_file.split('.json')[0].replace('\\', '/').split('/')[-1].replace(
        '_messages', ''
    )

    # Echo
    print(f'Reading data from channel -> {channel_name}')

    # read JSON file

    try: 
        with open(json_file, encoding='utf-8', mode='r') as fl:
            obj = json.load(fl)
            fl.close()

    except json.JSONDecodeError as e:
        print("JSON Decoding Error: Skipping this file")

        pass

    '''

    Get actions
    '''


    try:

        actions = obj['count']

    except Exception as e:
        print(e)
        print('Skipping due to error - likely no valid object for the count.')
        pass


    posts = len(
        [
            i for i in obj['messages'] if 'message' in i.keys()
            and i['reply_to'] == None
        ]
    )
    replies = len(
        [
            i for i in obj['messages'] if 'message' in i.keys()
            and i['reply_to'] != None
        ]
    )
    other = len(
        [
            i for i in obj['messages'] if 'action' in i.keys()
        ]
    )

    '''

    Attrs: views, forwards, replies
    '''
    views = sum(
        [
            i['views'] for i in obj['messages']
            if 'views' in i.keys() and i['views'] != None
        ]
    )
    forwards = sum(
        [
            i['forwards'] for i in obj['messages']
            if 'forwards' in i.keys() and i['forwards'] != None
        ]
    )
    replies_received = sum(
        [
            i['replies']['replies'] for i in obj['messages']
            if 'replies' in i.keys() and i['replies'] != None
        ]
    )

    #columns = [
    #'username', 
    #'collected_apdbctions', 
    #'collected_posts', 
    #'replies', 
    #'other_actions', 
    #'number_views', 
    #'forwards', 
    #'replies_received'
    #]

    #data = pd.DataFrame(columns=columns)
    
    # Init values
    #data['collected_actions'] = 0
    #data['collected_posts'] = 0
    #data['replies'] = 0
    #data['other_actions'] = 0
    #data['number_views'] = 0
    #data['forwards'] = 0
    #data['replies_received'] = 0


    '''

    Reading posts
    '''
    messages = obj['messages']

    # main object
    response = {
        'channel_name': channel_name
    }


    data = pd.DataFrame({
    'username': channel_name,
    'id': [0],
    'collected_actions': [0],
    'collected_posts': [0],
    'replies': [0],
    'other_actions': [0],
    'number_views': [0],
    'forwards': [0],
    'replies_received': [0]
        })

    # Add values to dataset
    data.loc[data['username'] == channel_name, 'collected_actions'] = actions
    data.loc[data['username'] == channel_name, 'collected_posts'] = posts
    data.loc[data['username'] == channel_name, 'replies'] = replies
    data.loc[data['username'] == channel_name, 'other_actions'] = other
    data.loc[data['username'] == channel_name, 'number_views'] = views
    data.loc[data['username'] == channel_name, 'forwards'] = forwards
    data.loc[data['username'] == channel_name, 'replies_received'] = replies_received


    pbar = tqdm(desc='enumerate', total = len(messages))

    # testing stored as an array of key-value pairs in 'msg' to handle perfectly with msgspec 
    finals = {}

    full_messages = { 'msg': [] }

    for idx, item in enumerate(messages):
        '''

        Iterate posts
        '''
        if item['_'] == 'Message':

            # channel id
            response['channel_id'] = item['peer_id']['channel_id']

            # message id
            msg_id = item['id']
            response['msg_id'] = msg_id
            response['grouped_id'] = None if 'grouped_id' not in item or item['grouped_id'] is None else item['grouped_id'] 
                
            # add attrs
            response['message'] = item['message']

            # clean message
            msg = clean_msg(item['message'])


            response['cleaned_message'] = msg

            # timestamp
            date = item['date']
            response['date'] = date

            # signature and message link
            response['signature'] = \
                f'msg_iteration.{idx}.user.{channel_name}.post.{msg_id}'
            response['msg_link'] = f'https://t.me/{channel_name}/{msg_id}'

            # check peer
            response['msg_from_peer'] = None
            response['msg_from_id'] = None
            response = msg_attrs(item, response)

            # reactions
            response['views'] = 0 if item['views'] == None else item['views']
            response['number_replies'] = \
                item['replies']['replies'] if item['replies'] != None else 0
            response['number_forwards'] = 0 if item['forwards'] == None \
                else item['forwards']

            # Forward attrs

            forward_attrs = item['fwd_from'] if item['fwd_from'] else None
            response['is_forward'] = 1 if forward_attrs != None else 0

            response['forward_msg_from_peer_type'] = None
            response['forward_msg_from_peer_id'] = None
            response['forward_msg_from_peer_name'] = None
            response['forward_msg_date'] = None
            response['forward_msg_date_string'] = None
            response['forward_msg_link'] = None

            if forward_attrs:
                response = get_forward_attrs(msg=forward_attrs,
                                             res=response,
                                             channels_data=data)


            # Reply attrs
            response['is_reply'] = 0
            response['reply_to_msg_id'] = None
            response['reply_msg_link'] = None
            response = get_reply_attrs(
                item,
                response,
                channel_name
            )

            # Media
            response['contains_media'] = 1 if item['media'] != None else 0
            if 'media' in item.keys():
                response['media_type'] = None if item['media'] == None \
                    else item['media']['_']

            # URLs -> Constructor MessageMediaWebPage
            '''
            Type WebPage

            Source: https://core.telegram.org/constructor/messageMediaWebPage
            Telethon: https://tl.telethon.dev/constructors/web_page.html
            '''

            # what the fuck is happening here?


            response = get_url_attrs(item, response)


            # Media Document -> Constructor MessageMediaDocument
            '''
            Type Document

            Source: https://core.telegram.org/constructor/messageMediaDocument
            Telethon: https://tl.telethon.dev/constructors/document.html
            '''
            response['document_type'] = None
            response['document_id'] = None
            response['document_video_duration'] = None
            response['document_filename'] = None

            response = get_document_attrs(item['media'], response)

            # Polls attrs
            '''
            
            Type Poll

            Source: https://core.telegram.org/constructor/messageMediaPoll
                    Telethon: https://tl.telethon.dev/constructors/poll.html

            '''
            response['poll_id'] = None
            response['poll_question'] = None
            response['poll_total_voters'] = None
            response['poll_results'] = None
            response = get_poll_attrs(item['media'], response)

            # Contact attrs
            '''

            Type Contact

            Source: https://core.telegram.org/constructor/messageMediaContact
                    Telethon: https://tl.telethon.dev/constructors/message_media_contact.html
            '''
            response['contact_phone_number'] = None
            response['contact_name'] = None
            forward_attrs = item['fwd_from']        
            response['contact_userid'] = None
            response = get_contact_attrs(item['media'], response)

            # Geo attrs
            '''

            Type GeoPoint

            Source: https://core.telegram.org/constructor/messageMediaGeo
            Telethon:
            >	https://tl.telethon.dev/constructors/geo_point.html
            >	https://tl.telethon.dev/constructors/message_media_venue.html
                
            '''
            response['geo_type'] = None
            response['lat'] = None
            response['lng'] = None
            response['venue_id'] = None
            response['venue_type'] = None
            response['venue_title'] = None
            response['venue_address'] = None
            response['venue_provider'] = None
            response = get_geo_attrs(item['media'], response)

            # this acts as a filter AND json deserialization
            finals = TGResponse(response)

            # encode it (do we really need this?) 
            finals = msgspec.json.encode(finals)
        

            #decode it back (again, do we really need this? might be overkilling?)
            finals = msgspec.json.decode(finals)

            # append it to 'msg' for idx, item in enumerate(messages):

            full_messages['msg'].append(finals)

            channel_name = response['channel_name']

            channel_path = os.path.join('./output/data/', f'{channel_name}/', f'{channel_name}_flat_messages.csv')
            pbar.update()

    return(full_messages, data)







def some_function(full_messages):

    # if .csv:
    if args['type'] == '.csv':

        try:
            with open(channel_path, mode='a+', newline='') as file:

                # debug here:
                #full = full_messages['msg'][0]['bulk_channel']['channel_name']

                ##
                pdb.set_trace()

                single = full_messages['msg'][0]
                ch_name = single['channel_name']
                csv_field_names = ch_name.keys()
                writer = csv.DictWriter(file, fieldnames=csv_field_names)
                writer.writeheader()
                for one in full_messages['msg']:
                    fin = one['channel_name']
                    writer.writerow(fin)


        except Exception as e:
            print(e)
            pass

                    
    # if output = .hdf5
    if args['type'] == '.hdf5':
            
        try:
            with h5py.File('data.hdf5', 'w') as file:

                channel_group = file.create_group(channel_name)

                for idx, message in enumerate(full_messages['msg']):
                    message_group = channel_group.create_group(f'message_{msg["id"]}')

        except Exception as e:
            print(e)
            pass
