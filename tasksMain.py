import pandas as pd
import numpy as np

#LOCATIONAL NATURE OF POWER PRICES
#TASK 1
#loads in prices csv files as dataframes
prices2016_df=pd.read_csv('ERCOT_DA_Prices_2016.csv')
prices2017_df=pd.read_csv('ERCOT_DA_Prices_2017.csv')
prices2018_df=pd.read_csv('ERCOT_DA_Prices_2018.csv')
prices2019_df=pd.read_csv('ERCOT_DA_Prices_2019.csv')

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
#avgPrice.to_csv('AveragePriceByMonth.csv',sep=' ',index=False)

#PRICE VOLATILITY
#TASK 4
#removes everything in SettlementPoint column that starts with "LZ_"
hubsOnly=mergedPrices_df[~mergedPrices_df["SettlementPoint"].astype(str).str.startswith('LZ_')]
#removes prices that are 0 or negative
hubsOnly=hubsOnly[hubsOnly['Price'] > 0]
#calculates hourly price volatility per year per settlement hub
hourlyVolatility=hubsOnly.groupby([pd.Grouper(key='Date',freq='Y'),'SettlementPoint'])['Price'].rolling(window=2).std().reset_index(name='HourlyVolatility')
#logReturns=np.log(hubsOnly['Price']/hubsOnly['Price'].shift(1)).reset_index(name='LogReturns')


#TASK 5
#adds year column
hourlyVolatility['Year']=hourlyVolatility['Date'].dt.year
#remove original date column
hourlyVolatility=hourlyVolatility.drop(columns=['Date'])
#rearrange columns
hourlyVolatility=hourlyVolatility[['SettlementPoint','Year','HourlyVolatility']]
#export to csv
#hourlyVolatility.to_csv('HourlyVolatilityByYear.csv',sep=' ',index=False)

#TASK 6
#find which hub had the maximum hourly volatility for each year
maxVolatility=hourlyVolatility.groupby([hourlyVolatility['SettlementPoint'],hourlyVolatility['Year']])['HourlyVolatility'].max()
#print(maxVolatility.head)
#export to csv
maxVolatility.to_csv('MaxVolatilityByYear.csv',sep=' ',index=False)

#DATA TRANSLATION AND FORMATTING
#Task 7
#we need to translate the data. X is the hour (1-24). date column goes by day
#so basically the price column becomes a row
#i'd need to extract all the data attached to each settlementpoint
#and then, within each settlement point csv, each row represents a new day, and each column represents an hour
#so the prices over the course of a day are put in a row
#ie the price at HB_BUSAVG on 2017-01-01 on hour X8 is 25.48
#am i going to brute force this? yes. yes i am. it's been like 3.5 hours with ZERO BREAKS
HBBusAvg=mergedPrices_df[mergedPrices_df['SettlementPoint'] == 'HB_BUSAVG']
HBHouston=mergedPrices_df[mergedPrices_df['SettlementPoint'] == 'HB_HOUSTON']
HBHubAvg=mergedPrices_df[mergedPrices_df['SettlementPoint'] == 'HB_HUBAVG']
HBNorth=mergedPrices_df[mergedPrices_df['SettlementPoint'] == 'HB_NORTH']
HBSouth=mergedPrices_df[mergedPrices_df['SettlementPoint'] == 'HB_SOUTH']
HBWest=mergedPrices_df[mergedPrices_df['SettlementPoint'] == 'HB_WEST']
LZAen=mergedPrices_df[mergedPrices_df['SettlementPoint'] == 'LZ_AEN']
LZCps=mergedPrices_df[mergedPrices_df['SettlementPoint'] == 'LZ_CPS']
LZHouston=mergedPrices_df[mergedPrices_df['SettlementPoint'] == 'LZ_HOUSTON']
LZLcra=mergedPrices_df[mergedPrices_df['SettlementPoint'] == 'LC_LCRA']
LZNorth=mergedPrices_df[mergedPrices_df['SettlementPoint'] == 'LZ_NORTH']
LZRaybn=mergedPrices_df[mergedPrices_df['SettlementPoint'] == 'LZ_RAYBN']
LZSouth=mergedPrices_df[mergedPrices_df['SettlementPoint'] == 'LZ_SOUTH']
LZWest=mergedPrices_df[mergedPrices_df['SettlementPoint'] == 'LZ_WEST']

#now we put the hours on the top and make each day its own row
#this is horrible i'm so sorry
#i probably could've used a for loop or something here and iterated through an index of varnames but i am running out of time
HBBusAvg_tposed=HBBusAvg.pivot_table(index=HBBusAvg["Date"].dt.date, columns=HBBusAvg["Date"].dt.hour, values="Price")
HBBusAvg_tposed.to_csv('formattedSpotHistory/spot_HBBUSAVG.csv',sep=' ',index=False)

HBHouston_tposed=HBHouston.pivot_table(index=HBHouston["Date"].dt.date, columns=HBHouston["Date"].dt.hour, values="Price")
HBHouston_tposed.to_csv('formattedSpotHistory/spot_HBHOUSTON.csv',sep=' ',index=False)

HBHubAvg_tposed=HBHubAvg.pivot_table(index=HBHubAvg["Date"].dt.date, columns=HBHubAvg["Date"].dt.hour, values="Price")
HBHouston_tposed.to_csv('formattedSpotHistory/spot_HBHUBAVG.csv',sep=' ',index=False)

HBNorth_tposed=HBNorth.pivot_table(index=HBNorth["Date"].dt.date, columns=HBNorth["Date"].dt.hour, values="Price")
HBHouston_tposed.to_csv('formattedSpotHistory/spot_HBNORTH.csv',sep=' ',index=False)

HBSouth_tposed=HBSouth.pivot_table(index=HBSouth["Date"].dt.date, columns=HBSouth["Date"].dt.hour, values="Price")
HBHouston_tposed.to_csv('formattedSpotHistory/spot_HBSOUTH.csv',sep=' ',index=False)

HBWest_tposed=HBWest.pivot_table(index=HBWest["Date"].dt.date, columns=HBWest["Date"].dt.hour, values="Price")
HBHouston_tposed.to_csv('formattedSpotHistory/spot_HBWEST.csv',sep=' ',index=False)

LZAen_tposed=LZAen.pivot_table(index=LZAen["Date"].dt.date, columns=LZAen["Date"].dt.hour, values="Price")
HBHouston_tposed.to_csv('formattedSpotHistory/spot_LZAEN.csv',sep=' ',index=False)

LZCps_tposed=LZCps.pivot_table(index=LZCps["Date"].dt.date, columns=LZCps["Date"].dt.hour, values="Price")
HBHouston_tposed.to_csv('formattedSpotHistory/spot_LZCPS.csv',sep=' ',index=False)

LZHouston_tposed=LZHouston.pivot_table(index=LZHouston["Date"].dt.date, columns=LZHouston["Date"].dt.hour, values="Price")
HBHouston_tposed.to_csv('formattedSpotHistory/spot_LZHOUSTON.csv',sep=' ',index=False)

LZLcra_tposed=LZLcra.pivot_table(index=LZLcra["Date"].dt.date, columns=LZLcra["Date"].dt.hour, values="Price")
HBHouston_tposed.to_csv('formattedSpotHistory/spot_LZLCRA.csv',sep=' ',index=False)

LZNorth_tposed=LZNorth.pivot_table(index=LZNorth["Date"].dt.date, columns=LZNorth["Date"].dt.hour, values="Price")
HBHouston_tposed.to_csv('formattedSpotHistory/spot_LZNORTH.csv',sep=' ',index=False)

LZRaybn_tposed=LZRaybn.pivot_table(index=LZRaybn["Date"].dt.date, columns=LZRaybn["Date"].dt.hour, values="Price")
HBHouston_tposed.to_csv('formattedSpotHistory/spot_LZRAYBN.csv',sep=' ',index=False)

LZSouth_tposed=LZSouth.pivot_table(index=LZSouth["Date"].dt.date, columns=LZSouth["Date"].dt.hour, values="Price")
HBHouston_tposed.to_csv('formattedSpotHistory/spot_LZSOUTH.csv',sep=' ',index=False)

LZWest_tposed=LZWest.pivot_table(index=LZWest["Date"].dt.date, columns=LZWest["Date"].dt.hour, values="Price")
HBHouston_tposed.to_csv('formattedSpotHistory/spot_LZWEST.csv',sep=' ',index=False)