import pandas as pd
import requests
import json
import datetime

def stations_info_api_extract():
    url='https://gbfs.bcycle.com/bcycle_indego/station_information.json'
    response=requests.get(url)
    if response.status_code==200:
        data=response.content.decode("utf-8")
        return data
    # RAISE AN ERROR, FORCE RETRY
    response.raise_for_status()
    return None

def stations_info_api_transform(data):
    # data: json
    stations_data=json.loads(data)['data']['stations']
    stations=[]
    for station in stations_data:
        s={}
        s['station_id']=station['station_id'][-4:]
        s['name']=station['name']
        s['lon']=station['lon']
        s['lat']=station['lat']
        s['station_type']=station['_bcycle_station_type']
        s['address']=station['address']
        s['active_status']=True
        stations.append(s)
    # stations_df=pd.DataFrame(stations)
    return stations