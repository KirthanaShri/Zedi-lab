""" POWER AND WEATHER DATA SEGREGATION SOURCE """

'''   * Importing Libraries *   '''

import numpy as np
import pandas as pd

import datetime as dt

import glob
import os


'''   * DATA COLLECTION *  '''

#Mounting google drive (USE ONLY IN COLAB)
from google.colab import drive
drive.mount('/content/drive')


'''   * DATA PRE-PROCESSING *   '''

def naming(df):
    ''' Function to describe ID of line voltage, current and frequency data'''

    # Renaming column names.
   df = df.rename(columns = {0:"DEVICEID",1:"CITY",2:"LOCATION",3:"ID"})
   df.sort_values('DEVICEID',inplace=True,ignore_index=True)

   # 1,2,3 - line voltage and current , 4,5,6- Frequency and 7,8 - other sensor
   df.loc[(df["ID"]==1) | (df["ID"]==4),'ID'] = 'R'
   df.loc[(df["ID"]==2) | (df["ID"]==5),'ID'] = 'G'
   df.loc[(df["ID"]==3) | (df["ID"]==6),'ID'] = 'B'
   df.loc[(df["ID"]==7) | (df["ID"]==8),'ID'] = 'O'

   return df


def epoch_to_dt(dfx):
  '''  Function to convert epoch time in seconds to date and time'''

  dfx['EPOCH'] = pd.to_datetime(dfx['EPOCH'], unit='s')
  dfx['date'] = dfx['EPOCH'].dt.date
  dfx['time'] = dfx['EPOCH'].dt.time
  #   dfx=dfx.drop(['EPOCH'],axis=1)

'''
import datetime
def epoch_to_dt(dfx):
   dfx['Date_time'] = dfx['EPOCH'].apply(datetime.datetime.fromtimestamp)
'''


def segregate(df):
   ''' Function to store line voltage, current and frequency data in a matrix using data frame'''

   # Storing I(current),V(voltage),EPOCH in a dataframe 'dfl'.
   dfl = df.dropna()

   # Raw data file format from AWS is Device ID, City, Location, ID
   # followed by the Unix timestamp current, Unix timestamp voltage, for 60 values stored at every 1second.
   dfl = dfl.set_index(["DEVICEID", "CITY", "LOCATION", "ID"])
   colum = ['I', 'V', 'EPOCH'] * 60
   subcol = [i for i in range(1, 61)] * 3
   subcol.sort()

   # Multi-level Stacking of I, V date and time wise
   multicol = pd.MultiIndex.from_tuples(list(zip(colum, subcol)))
   dfl.columns = multicol
   dfl = dfl.stack()
   epoch_to_dt(dfl)
   dfl = dfl.drop(['EPOCH'], axis=1)
   dfl = dfl.set_index(["date", "time"], append=True)

   # Storing FREQ(frequency),EPOCH in a dataframe 'dfr'.
   dfr = df[(df[124].isnull()) & (df[68].notna())]
   dfr = dfr.dropna(axis=1)

   # Raw data file format from AWS is Device ID, City, Location, ID
   # followed by the Unix timestamp frequency for 60 values stored at every 1second.
   dfr = dfr.set_index(["DEVICEID", "CITY", "LOCATION", "ID"])
   colum1 = ['FREQ', 'EPOCH'] * 60
   subcol1 = [i for i in range(1, 61)] * 2
   subcol1.sort()

   # Multi-level Stacking of FREQ date and time wise
   multicol1 = pd.MultiIndex.from_tuples(list(zip(colum1, subcol1)))
   dfr.columns = multicol1
   dfr = dfr.stack()
   epoch_to_dt(dfr)
   dfr = dfr.drop(['EPOCH'], axis=1)
   dfr = dfr.set_index(["date", "time"], append=True)

   # Storing other sensors,EPOCH in a dataframe 'dfu'.
   dfu = df[df[68].isnull()]
   dfu = dfu.dropna(axis=1)

   # Raw data file format from AWS is Device ID, City, Location, ID
   # followed by Ultra-Sonic, Light values, Temperature, Humidity, Pressure, PIR, CO2,
   dfu = dfu.set_index(["DEVICEID", "CITY", "LOCATION", "ID"])
   dfu.columns = ['US', 'EPOCH'] * 15 + ['LIGHT', 'EPOCH'] * 10 + ['BME_280_TEMP', 'BME_280_HUM', 'BME_280_PRESS'] + [
      'BME_680_TEMP', 'BME_680_HUM', 'BME_680_PRESS', 'BME_680_GAS'] + ['PIR1', 'PIR2', 'CO2'] + ['THER_1', 'THER_2',
                                                                                                  'THER_3', 'EPOCH']
   # Merging dfl and dfr
   dfs = pd.merge(dfl, dfr, how='outer', on=["DEVICEID", "CITY", "LOCATION", "ID", "date", "time"])
   dfs.reset_index(inplace=True)

   #returning merged dataframe dfs and other sensors dataframe dfu
   return dfs, dfu


def filing(dfs):
   ''' function to store data in a directory having DEVICEID as filenames
   opening to I,V,FREQ folder having date-wise files containing data stored at every 1 second

   DEVICEID folder --> I,V,FREQ folder --> date csv files'''

   try:
      filenames = dfs['DEVICEID'].unique()
      for file in filenames:
         # Given path is to store data in Google MyDRIVE in the given format
         path = "/content/drive/MyDrive/filing/".format(file=file)
         path = os.path.join(path, str(file), "I,V,FREQ")
         dates = dfs['date'].unique()

         #When deviceid folder does not already exists, new deviceid path directory is created and used as current
         # directory to save entries by creating a date folder
         if not os.path.exists(path):
            os.makedirs(path)
            os.chdir(path)
            for dtt in dates:
               path = os.path.join(path, str(dtt) + ".csv")
               dfs.loc[(dfs['DEVICEID'] == file) & (dfs['date'] == dtt)].to_csv(str(dtt) + ".csv", index=False)

         #when deviceid folder already exists, deviceid path directory is used as current directory and
         #when date folder does not already exists, date folder is created to save entries and
         #when date folder exists, new entries are added to the folder by concatenation.
         else:
            os.chdir(path)
            for dtt in dates:
               path = os.path.join(path, str(dtt) + ".csv")
               if not os.path.exists(path):
                  dfs.loc[(dfs['DEVICEID'] == file) & (dfs['date'] == dtt)].to_csv(str(dtt) + ".csv", index=False)
               else:
                  df_new = pd.concat([pd.read_csv(path), dfs.loc[(dfs['DEVICEID'] == file) & (dfs['date'] == dtt)]],
                                     ignore_index=True)
                  df_new = df.sort_values(by=['time'])
                  df_new.to_csv(path, index=False)
   except KeyError:
      pass


#List of raw AWS csv files retrieved from paths recursively in joined_list.
path =  "/content/drive/MyDrive/csv/RawSample/*"
joined_list = glob.glob(path)

#Concatenating dataframes that are created by reading files in joined_list.
df=pd.concat([pd.read_csv(file,sep='|',names=[i for i in np.arange(184)])for file in joined_list[:(len(joined_list)//2)]],ignore_index=True)
df2=pd.concat([pd.read_csv(file,sep='|',names=[i for i in np.arange(184)])for file in joined_list[(len(joined_list)//2):]],ignore_index=True)
#print(df)
#print(df2)

#Calling naming function.
df =naming(df)
#print(df)

df2=naming(df2)
#print(df2)

#Calling segregate function
dfs,dfu=segregate(df)
dfs2,dfu2=segregate(df2)


dfs = pd.concat([dfs,dfs2])
#print(dfs)
