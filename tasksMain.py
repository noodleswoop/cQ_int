import pandas as pd
import numpy as np

#LOCATIONAL NATURE OF POWER PRICES
#TASK 1
#loads in prices csv files as dataframes
prices2016_df=pd.read_csv('ERCOT_DA_Prices_2016.csv')
prices2017_df=pd.read_csv('ERCOT_DA_Prices_2017.csv')
prices2018_df=pd.read_csv('ERCOT_DA_Prices_2018.csv')
prices2019_df=pd.read_csv('ERCOT_DA_Prices_2019.csv')

#troubleshooting
#shape2016=prices2016_df.shape
#shape2017=prices2017_df.shape
#shape2018=prices2018_df.shape
#shape2019=prices2019_df.shape
#totalshape=shape2016+shape2017+shape2018+shape2019

#merges the data from all four years into one single dataframe
mergedPrices_df=pd.concat([prices2016_df,prices2017_df,prices2018_df,prices2019_df])

#TASK 2
#find the average of all of the prices for a given settlement point during a specific year-month
#for example: average price of HB_BUSAVG for 2016-01, 2016-02, etc
#prices are checked every hour over a 24 hour period
#can partition the data by year-month
#and then find the average of each SettlementPoint within that year-month

#makes sure the date column is formatted as date time
mergedPrices_df['Date']=pd.to_datetime(mergedPrices_df['Date'])
#calculates average price
avgPrice=mergedPrices_df.groupby([pd.Grouper(key='Date',freq='M'),'SettlementPoint'])['Price'].mean().reset_index(name='AveragePrice')

#TASK 3
#need to format data to get desired output
#adds year and month columns
avgPrice['Year']=avgPrice['Date'].dt.year
avgPrice['Month']=avgPrice['Date'].dt.month
#remove original date column
avgPrice=avgPrice.drop(columns=['Date'])
#rearrange columns
avgPrice=avgPrice[['SettlementPoint','Year','Month','AveragePrice']]
#export to csv
avgPrice.to_csv('AveragePriceByMonth.csv',sep=' ',index=False)

#PRICE VOLATILITY
#TASK 4