# -*- coding: utf-8 -*-
"""
Created on Mon Feb 13 16:22:47 2023

@author: FcHall
"""

# General
import pandas as pd
import numpy as np
import math
import traceback

# Time Formatting
import time
from datetime import timedelta, datetime
import pytz

# API
import requests
import json
import Constants

# sql
import pyodbc
from sqlalchemy import create_engine
import urllib

# APscheduler
from  apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.date import DateTrigger
sched = BackgroundScheduler()

import warnings
warnings.filterwarnings('ignore')


###############################################################################
######### CREDENTIALS

_SERVER = 'https://home.sensibo.com/api/v2'
api_key = Constants.sensibo_key # heatpumprate account

###############################################################################

'''
Does the cchpACStates table have all of these rows?

1. update cchpACStates (run sensiboData.py file)
2. get last state for each deviceID
3. if on: 
    schedule peak

4. reset to previous state - filtering out non-API adjustments



acState: {
on*: boolean                            true for on, false for off
mode*: string                           one of modes in remoteCapabilities ("cool", "heat", "fan", "auto", "dry")
fanLevel*: string                       one of fan levels in remoteCapabilities (e.g., "low", "medium", "high", "auto")
targetTemperature*: integer             target temperature
temperatureUnit: string                 'C' for Celsius or 'F' for Fahrenheit
swing: string                           one of the swing states in remoteCapabilities (e.g., "stopped", "rangeful")
}

'''
###############################################################################
        
        
def writePeak(peak):
    quoted = urllib.parse.quote_plus("driver={SQL Server};server=bedmssql-dev;database=test_fchall;trusted_connection=yes")
    engine = create_engine('mssql+pyodbc:///?odbc_connect={}'.format(quoted))
    peak.to_sql('cchpPeaks', con = engine,if_exists='append',index=False) # append new data


def latestStates():
    conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=bedmssql-dev;'
                      'Database=test_fchall;'
                      'Trusted_Connection=yes;')
    
    acState = pd.read_sql_query("SELECT * FROM[test_fchall].[dbo].[cchpACState] t1 \
                                 WHERE EXISTS(SELECT 1 FROM [test_fchall].[dbo].[cchpACState] t2 \
                                 WHERE t2.deviceID = t1.deviceID \
                                 AND reason in ('ExternalIrCommand',\
                                                'ChangedRemote',\
                                                'UserRequest',\
                                                'Trigger', \
                                                'StateCorrectionByUser') \
                                GROUP BY t2.deviceID \
                                HAVING t1.interval = MAX(t2.interval))",conn)
    return acState

def peakScheduler(device, acState, deltaT, duration, mode, returnState):
    '''
    check if device is on
    call peak
    call aftermath
    '''
    acState = acState[acState.deviceID == device].reset_index(drop=True).to_dict()

    
    if 'heat' in mode: # decrease setpoint
        if acState['temperatureUnit'][0] == 'C':
            newT = int(acState['targetTemperature'][0] - deltaT*5/9)
            print('celsius change')
        else:
            newT = int(acState['targetTemperature'][0] - deltaT)
            
    if 'cool' in mode: # increase setpoint
        if acState['temperatureUnit'][0] == 'C':
            newT = int(acState['targetTemperature'][0] + deltaT*5/9)
            print('celsius change')
            
        else:
            newT = int(acState['targetTemperature'][0] + deltaT)
    
    # print(newT)
    
    reset = {"minutesFromNow":duration,
                   "acState":{
                       "on" : True,
                       "mode" : acState['mode'][0],
                       "fan" : acState['fanLevel'][0],
                       "targetTemperature" : int(acState['targetTemperature'][0]),
                       "temperatureUnit" : acState['temperatureUnit'][0],
                       "swing" : acState['swing'][0]}}
    
    newData = {"acState":{
        "on" : True,
        "mode" : acState['mode'][0],
        "fanLevel" : acState['fanLevel'][0],
        "targetTemperature" : newT,
        "temperatureUnit" : acState['temperatureUnit'][0],
        "swing" : acState['swing'][0]}}
    
    
    if acState['on'][0] == '1' and acState['mode'][0] in mode: # check if on and mode aligns
        peakCaller(device,newData) # call peak
        print(f'called peak for {device}')
        if returnState == True:
            aftermath(device, reset) # schedule aftermath
    else:
        print(f'no peak for {device} b/c hp is off or mode is different...')

def peakCaller(device, data):
    print(data)
    print(api_key)
    response = requests.post(f'https://home.sensibo.com/api/v2/pods/{device}/acStates?apiKey={api_key}', json=data)
    print(response)

def aftermath(device, data):
    '''
    Set timer to return device 
    to previous AC state
    '''
    # print(data)
    response = requests.put(f'https://home.sensibo.com/api/v1/pods/{device}/timer?apiKey={api_key}', json=data)
    result = json.loads(response.content.decode('utf-8'))
    print('peak scheduler is a',result['status'])
    
###############################################################################
##### PEAK SCHEDUKER - RUN AT BEGINNING OF PEAK
###############################################################################

def schedule_peak(start,mode,duration,deltaT,returnState):

    sched.add_job(peak, DateTrigger(run_date=start),
                  args=['on',start,mode,duration,deltaT,returnState])
    
    print(sched.get_jobs())
    sched.start()


def peak(peak,start,mode,peakDuration,deltaT,returnState):
    
    exec(open("sensiboACData.py").read()) # execute Sensibo Data
    acStates = latestStates() # reduce to latest non API called state
    
    createDate = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    temperatureUnit = 'F'
    fanChange = 0
    peakType = 'fcm'
    notes = 'APschedule'
    fanChange = 0

    
    if peak == 'on':
        for i, device in enumerate(acStates.deviceID):
    
            print(f'\n {i}. calling a peak for {device}')
            try:
                peakScheduler(device,acStates,deltaT,peakDuration,mode,returnState)
            except Exception:
                traceback.print_exc()
        
        entry = {'createDate': [createDate],
                'datetimeStart': [start],
                'datetimeEnd': [start],
                'mode': 'cool',
                'tChange': [deltaT],
                'temperatureUnit': [temperatureUnit],
                'fanChange': [fanChange],
                'peakType': [peakType],
                'notes': [notes]
                }
        
        peakEntry = pd.DataFrame(entry)
        writePeak(peakEntry) # write to sql


preCool = datetime.strptime('2023-10-05 17:00:00', '%Y-%m-%d %H:%M:%S')
event = datetime.strptime('2023-10-05 18:00:00', '%Y-%m-%d %H:%M:%S')
preCool = preCool.astimezone(pytz.timezone('America/New_York')).strftime('%Y-%m-%d %H:%M:%S') # Convert to EST
event = event.astimezone(pytz.timezone('America/New_York')).strftime('%Y-%m-%d %H:%M:%S')

schedule_peak(preCool,['cool','dry'],60,-3,False) # pre-cool
schedule_peak(event,['cool','dry'],120,3,True) # actual event