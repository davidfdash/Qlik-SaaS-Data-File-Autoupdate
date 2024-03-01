import time
import pathlib
import requests
import os
import config
import json
import threading


def update_file_excel(data):
    while True:
        for i in data:
            if data[i]['upload_flag'] == '1':
                url = config.cloud_url + "api/v1/data-files/" + data[i]['id']
                payload = {}
                files = [
                    ('file', (data[i]['filename'], open(data[i]['path'], 'rb'),
                              'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'))
                ]
                headers = {
                    'Authorization': 'Bearer ' + config.api_key,
                    'Accept': 'application/json'
                }
                response = requests.request("PUT", url, headers=headers, data=payload, files=files)
                print(response.text)
        print('Excel update cycle complete')
        time.sleep(30)


def update_file_qvd(data):
    while True:
        for i in data:
            if data[i]['upload_flag'] == '1':
                url = config.cloud_url + "api/v1/data-files/" + data[i]['id']
                payload = {}
                files = [
                  ('file', (data[i]['filename'], open(data[i]['path'], 'rb'), 'application/octet-stream'))
                ]
                headers = {
                  'Authorization': 'Bearer ' + config.api_key,
                  'Accept': 'application/json'
                }
                response = requests.request("PUT", url, headers=headers, data=payload, files=files)
                print(response.text)
        print('QVD update cycle complete')
        time.sleep(30)


def data_maint():
    while True:
        #add new items to data store dictionary
        for path, dirs, files in os.walk(config.share_root_drive):
            for file in files:
                if file not in dataStore:
                    fileAndPath = os.path.join(path, file)
                    f_name = pathlib.Path(fileAndPath)
                    m_timestamp = f_name.stat().st_mtime
                    fileExt = file.split('.')
                    flag = '0'
                    dataStore[file] = {'filename': file, 'path': fileAndPath, 'timestamp': m_timestamp, 'fileExtension': fileExt[-1], 'upload_flag': flag}

        #update datastore with data file id's
        url = config.cloud_url + "api/v1/data-files"
        headers = {
            'Authorization': 'Bearer ' + config.api_key,
            'Accept': 'application/json'
        }
        response = requests.request("GET", url, headers=headers)
        data = json.loads(response.text)
        for i in data['data']:
            id = i['id']
            filename = i['name']
            dataStore[filename]['id'] = id

        #update flag to 1 if last modified time is later than previously stored
        for path, dirs, files in os.walk(config.share_root_drive):
            for file in files:
                fileAndPath = os.path.join(path, file)
                f_name = pathlib.Path(fileAndPath)
                m_timestamp = f_name.stat().st_mtime
                if file in dataStore:
                    if m_timestamp > dataStore[file]['timestamp']:
                        dataStore[file]['timestamp'] = m_timestamp
                        dataStore[file]['upload_flag'] = '1'

        #Split datastore into qvd and other
        for i in dataStore:
            if dataStore[i]['fileExtension'] == 'qvd':
                qvdStore[i] = dataStore[i]
            else:
                excelStore[i] = dataStore[i]
        print(qvdStore)
        print('_________________')
        print(excelStore)
        time.sleep(30)


dataStore = {}
qvdStore = {}
excelStore = {}
t1 = threading.Thread(target=data_maint)
t2 = threading.Thread(target=update_file_excel, args=(excelStore,))
t3 = threading.Thread(target=update_file_qvd, args=(qvdStore,))

t1.start()
t2.start()
t3.start()
