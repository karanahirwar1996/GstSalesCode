import pandas as pd
import numpy as np
import os
import gspread
import warnings
from datetime import datetime, timedelta
from gspread_dataframe import set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials
import drive2
b2c_raw_df=drive2.drive_libV2("1aPvFEm_mp8AkVlDhyaHJdcBwxA8K3h9a")
b2b_raw_df=drive2.drive_libV2("122vUXdudsI483QU2FWsjTGz3X5M3doPY")
b2c_raw_df["Channel"]="B2C"
b2b_raw_df["Channel"]="B2B"
raw_df=pd.concat([b2c_raw_df,b2b_raw_df],ignore_index=True)
raw_sort=raw_df[['Invoice Number', 'Invoice Date', 'Transaction Type','Order Id','Shipment Date', 'Order Date', \
        'Quantity', 'Asin','Ship From State', 'Ship To State', 'Invoice Amount', 'Tax Exclusive Gross','Warehouse Id',"Channel"]]
filter_raw=raw_sort[(raw_sort['Transaction Type']=="Shipment")|(raw_sort['Transaction Type']=="Refund")].copy()
filter_raw["Month"]=pd.to_datetime(filter_raw["Invoice Date"]).dt.strftime("%B")
filter_raw["Invoice Date"]=pd.to_datetime(filter_raw["Invoice Date"]).dt.strftime('%d-%m-%Y')
filter_raw["Shipment Date"]=pd.to_datetime(filter_raw["Shipment Date"]).dt.strftime('%d-%m-%Y')
filter_raw["Order Date"]=pd.to_datetime(filter_raw["Order Date"]).dt.strftime('%d-%m-%Y')
filter_raw["Link"]=filter_raw["Ship From State"]+" - "+filter_raw["Ship To State"]
filter_raw["Warehouse Id"]=filter_raw["Warehouse Id"].fillna("N/a")
pivot_df=filter_raw.groupby(['Invoice Number', 'Invoice Date', 'Transaction Type', 'Order Id',\
                    'Shipment Date', 'Order Date','Asin', 'Ship From State','Ship To State',\
                    'Warehouse Id', 'Channel', 'Month', 'Link'])[["Quantity",'Invoice Amount', 'Tax Exclusive Gross',]].sum().reset_index()
#pivot_df["O2D"]=(pivot_df["Shipment Date"]-pivot_df["Order Date"]).dt.days
pivot_df.rename({"Asin":"Channel ID","Invoice Amount":"Total Sales","Tax Exclusive Gross":"Net Sales"},axis=1,inplace=True)
def find_region(state):
    regi_1=["CHANDIGARH","DELHI","HARYANA","HIMACHAL PRADESH","JAMMU & KASHMIR", "PUNJAB","RAJASTHAN","UTTAR PRADESH","UTTARAKHAND"]
    regi_2=["DADRA AND NAGAR HAVELI AND DAMAN AND DIU","MADHYA PRADESH",'GUJARAT', 'MAHARASHTRA']
    regi_3=["ANDAMAN & NICOBAR ISLANDS", "ANDHRA PRADESH","GOA","KARNATAKA","KERALA","PUDUCHERRY",'TAMIL NADU',"TELANGANA", "Lakshadweep"]
    regi_4=['ARUNACHAL PRADESH', 'MIZORAM', 'NAGALAND',"ASSAM","BIHAR","CHHATTISGARH","JHARKHAND", "MANIPUR","MEGHALAYA","ODISHA", "Sikkim","Tripura","WEST BENGAL"]
    if state.upper() in regi_1:
        return "Region 1"
    elif state.upper() in regi_2:
        return "Region 2"
    elif state.upper() in regi_3:
        return "Region 3"
    else:
        return "Region 4"
pivot_df["Region"]=pivot_df["Ship To State"].apply(find_region)
scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name('./original-advice-385307-e221975bf7db.json', scope)
client = gspread.authorize(creds)
gs = client.open('Cladd Account Workings')
link_sheet=gs.worksheet('Linking')
all_data_link=link_sheet.get_all_records()
Raw_link_df=pd.DataFrame(all_data_link)
sort_link_df=Raw_link_df[['Channel ID',"Item Name",'Parent Category', 'Child Category']].copy()
add_item_pivot=pivot_df.merge(sort_link_df,on="Channel ID",how="left")
gs = client.open('Cladd_Sales_Analysis_2023')
sheet1=gs.worksheet('Sales')
sheet1.clear()
set_with_dataframe(sheet1,add_item_pivot)
