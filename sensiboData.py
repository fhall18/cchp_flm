# -*- coding: utf-8 -*-
"""
Created on Fri Feb 10 13:10:35 2023

@author: FcHall
"""


import pandas as pd
import requests
import json
import numpy as np

# SQL
import pyodbc
import urllib
from sqlalchemy import create_engine

import win32com.client # email

import warnings
import Constants
warnings.filterwarnings('ignore')

###############################################################################
######### CREDENTIALS

_SERVER = 'https://home.sensibo.com/api/v2'
api_key = Constants.sensibo_key # heatpumprate account

###############################################################################
######### FUNCTIONS

def get_devices():
    response = requests.get(f'https://home.sensibo.com/api/v2/users/me/pods?apiKey={api_key}')
    return json.loads(response.content.decode('utf-8'))

def get_climate(device):
    '''
    This returns temperature and humidity in a very granular form for each sensibo device
        Parameters: Sensibo Device ID
        Return: dataframe with time, temp, hum, deviceID
    '''
    try:
        response = requests.get(f'https://home.sensibo.com/api/v2/pods/{device}/historicalMeasurements?days=1&apiKey={api_key}')   
        raw = json.loads(response.content.decode('utf-8'))
        
        t_raw = pd.DataFrame.from_dict(raw['result']['temperature'])
        t_raw.columns = ['interval','temperature']
    
        h_raw = pd.DataFrame.from_dict(raw['result']['humidity'])
        h_raw.columns = ['interval','humidity']
        h_raw['deviceID'] = device
        df_ = t_raw.merge(h_raw).reset_index(drop=True)
        df_['interval'] = pd.to_datetime(df_.interval).dt.tz_convert(None)
        df_ = df_[['deviceID', 'interval', 'temperature', 'humidity']]
        
        return df_
    except:
        print('no temp data')


def parse_acState(raw):
    '''
    Helper function for the get_acState()
        returns a cleaner dataframe
    '''
    
    result_raw =  pd.DataFrame.from_dict(raw['result'])
    
    # prase acState
    df_acState = pd.DataFrame.from_dict(list(result_raw.acState))
    df_acState = df_acState.iloc[:,1:]
    
    # bring it all back together
    df_ = result_raw[['id','status','changedProperties','reason','failureReason']]
    df_.changedProperties = df_.changedProperties.str.get(0)
    df_['interval'] = pd.to_datetime(list(pd.DataFrame.from_dict(list(result_raw.iloc[:,1])).iloc[:,0]))
    df_['interval'] = pd.to_datetime(df_.interval).dt.tz_convert(None)
    df_ = pd.concat([df_.reset_index(drop=True),df_acState.reset_index(drop=True)], axis=1)
    
    return df_    
                
def get_acState(device):
    try:
        response = requests.get(f'https://home.sensibo.com/api/v2/pods/{device}/acStates?&apiKey={api_key}')   
        raw = json.loads(response.content.decode('utf-8'))
        df = parse_acState(raw)
        df['deviceID'] = device
        df['eventID'] = df.id
        if 'horizontalSwing' not in df.columns:
            df['horizontalSwing'] = None
        if 'nativeTargetTemperature' not in df.columns:
            df['nativeTargetTemperature'] = None
        if 'nativeTemperatureUnit' not in df.columns:
            df['nativeTemperatureUnit'] = None
        if 'fanLevel' not in df.columns:
            df['fanLevel'] = None
        if 'targetTemperature' not in df.columns:
            df['targetTemperature'] = None
        if 'temperatureUnit' not in df.columns:
            df['temperatureUnit'] = None
        try:
        
            df = df[['eventID','deviceID', 'status', 'changedProperties', 'reason', 'failureReason','interval', 'on', 'mode', 'targetTemperature', 'temperatureUnit','nativeTargetTemperature', 'nativeTemperatureUnit', 'fanLevel', 'swing','horizontalSwing']]
            return df 
        except:
            print('FORMAT ISSUES')
    
    except:
        print('no new data')
        
def email_error(error_state):
    pass


def group_climate_pull(df):
    df = df.groupby(['deviceID','interval'],as_index = False).agg({'temperature':'mean','humidity':'mean'}).reset_index(drop=True)
    return df

###############################################################################
######### GET SQL

conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=bedmssql-dev;'
                      'Database=test_fchall;'
                      'Trusted_Connection=yes;')

devices = pd.read_sql_query("SELECT * FROM cchpDevices WHERE deviceType = 'sensibo'",conn)
climate = pd.read_sql_query("SELECT * FROM cchpClimate",conn)
acState = pd.read_sql_query("SELECT * FROM cchpACState",conn)

climateMaxInterval = pd.read_sql_query("SELECT deviceID, max(interval) as maxInterval FROM cchpClimate group by deviceID",conn) # may need to get eventID...
acEvents = list(acState.eventID)



###############################################################################
######### PULL NEW DATA

df_climate = pd.DataFrame()
df_acState = pd.DataFrame()

for row in range(devices.shape[0]):
    print(devices.deviceID[row])
    temp_climate = get_climate(devices.deviceID[row])
    df_climate = pd.concat([df_climate,temp_climate])
    
    temp_ac = get_acState(devices.deviceID[row])
    df_acState = pd.concat([df_acState,temp_ac])

###############################################################################
######### SET UP EMAIL

ol=win32com.client.Dispatch("outlook.application")
olmailitem=0x0 #size of the new email
newmail=ol.CreateItem(olmailitem)
newmail.Subject= 'Testing Mail'
newmail.To= Constants.email_address 

###############################################################################
######### SAVE DATA- CLIMATE

df_climate.head()

newClimate = df_climate.merge(climateMaxInterval, on = ['deviceID'], how='left') # merge max interval
newClimate.maxInterval = newClimate['maxInterval'].fillna(pd.to_datetime('1992-10-18')) # replace dates that don't have dates
newClimate = newClimate[newClimate.interval > newClimate.maxInterval] # filter max interval
newClimate = newClimate.drop('maxInterval',axis=1) # drop max interval column
newClimate = group_climate_pull(newClimate) # groups and averages if there are multiple rows with the same device and interval


print('old data:',climate.shape)
print('pulled data:',df_climate.shape)
print('new data:',newClimate.shape)
print('latest', max(climate.interval))

climate.groupby(['deviceID']).agg({'interval':'max'})
newClimate.groupby(['deviceID']).agg({'interval':'min'})

acBody = 'no ac data'
climateBody = 'no climate data'


# save if new
if newClimate.shape[0] != 0:
    newClimate = newClimate.sort_values(by='interval', ascending=True).reset_index(drop=True)
    print('new data for %s devices!' % len(set(newClimate.deviceID)))
    # save
    quoted = urllib.parse.quote_plus("driver={SQL Server};server=bedmssql-dev;database=test_fchall;trusted_connection=yes")
    engine = create_engine('mssql+pyodbc:///?odbc_connect={}'.format(quoted))
    # newClimate = newClimate.sort_values(by='interval', ascending=True)
    chunks = np.array_split(newClimate, 10)
    counter = 0
    for i in chunks:
            counter = counter + 1
            print('loading...',counter, max(i.interval))
            i.to_sql('cchpClimate', con = engine,if_exists='append',index=False) # append new data

        
    if counter == 10:
        climateBody = 'climate_successful'
    else:
        climateBody = 'climate_unsuccessful'

###############################################################################
######### SAVE DATA - AC STATE

newACState = df_acState[~df_acState.eventID.isin(acEvents)]

print('old data:',acState.shape)
print('pulled data:',df_acState.shape)
print('new data:',newACState.shape)


if newACState.shape[0] != 0:
    print('new data for %s devices!' % len(set(newACState.deviceID)))
    # save
    try:
        quoted = urllib.parse.quote_plus("driver={SQL Server};server=bedmssql-dev;database=test_fchall;trusted_connection=yes")
        engine = create_engine('mssql+pyodbc:///?odbc_connect={}'.format(quoted))
        newACState.to_sql('cchpACState', con = engine,if_exists='append',index=False) # append new data
        acBody = "ac_successful"
    except:
        acBody = "ac_unsuccessful"

newmail.Body = climateBody + ", " + acBody 
newmail.Send()
