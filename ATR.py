from vnstock import Listing, Quote, Vnstock, Company
import pandas as pd
from datetime import datetime, timedelta
import numpy as np
from time import sleep
def calculate_atr_percentage_VN100():
    end_date = datetime.now() 
    start_date = end_date - timedelta(days=10)
    start_date_str = start_date.strftime('%Y-%m-%d')
    end_date_str = end_date.strftime('%Y-%m-%d')
    group_stocks = Listing(source='VCI').symbols_by_group('VN100').tolist()
    existing_df = pd.read_csv('Top_VN100_All.csv')
    atr_data = []
    for sym in group_stocks:
        
        company = Company(source="vci", symbol=sym, random_agent=False, show_log=False)
        quote = Quote(symbol=sym, source='VCI')
        df_ov = company.overview()
        df = quote.history(start=start_date_str, end=end_date_str, interval='1D')
        df['high_low'] = df['high'] - df['low']
        df['high_close'] = np.abs(df['high'] - df['close'].shift(1))
        df['low_close'] = np.abs(df['low'] - df['close'].shift(1))
        df['true_range'] = df[['high_low', 'high_close', 'low_close']].max(axis=1)
        df['atr'] = df['true_range'].rolling(window=5).mean()
        df['atr_percentage'] = (df['atr'] / df['close']) * 100
        latest_atr = df[df['atr_percentage'].notna()].iloc[-1]['atr_percentage']
        atr_data.append({
                'symbol': sym,
                'atr_percentage': latest_atr
            })
        sleep(3)
    atr_df = pd.DataFrame(atr_data)
    if not existing_df.empty:
        
        updated_df = existing_df.merge(atr_df, on='symbol', how='left', suffixes=('', '_new'))
        
        if 'atr_percentage_new' in updated_df.columns:
            updated_df['atr_percentage'] = updated_df['atr_percentage_new'].fillna(updated_df.get('atr_percentage', np.nan))
            updated_df.drop('atr_percentage_new', axis=1, inplace=True)
    else:
        
        updated_df = atr_df
    updated_df.to_csv('Top_VN100_All.csv', index=False)  
    return updated_df  

def chia_atr_group_VN100():
    df= pd.read_csv('Top_VN100_All.csv')
    group1_raw = df.iloc[:30].copy()  
    group2_raw = df.iloc[30:60].copy()   
    group3_raw = df.iloc[60:].copy()
    group1 = group1_raw.sort_values('atr_percentage', ascending=True).reset_index(drop=True)
    group1['rank_atr'] = range(1, len(group1) + 1)
    group1.to_csv('Group1.csv', index=False)
    
    
    group2 = group2_raw.sort_values('atr_percentage', ascending=True).reset_index(drop=True)
    group2['rank_atr'] = range(1, len(group2) + 1)
    group2.to_csv('Group2.csv', index=False)
    
    
    group3 = group3_raw.sort_values('atr_percentage', ascending=True).reset_index(drop=True)
    group3['rank_atr'] = range(1, len(group3) + 1)
    group3.to_csv('Group3.csv', index=False)
    return group1, group2, group3

def calculate_atr_percentage_HNX30():
    end_date = datetime.now() 
    start_date = end_date - timedelta(days=10)
    start_date_str = start_date.strftime('%Y-%m-%d')
    end_date_str = end_date.strftime('%Y-%m-%d')
    group_stocks = Listing(source='VCI').symbols_by_group('HNX30').tolist()
    existing_df = pd.read_csv('Group4.csv')
    atr_data = []
    for sym in group_stocks:
        
        
        quote = Quote(symbol=sym, source='VCI')
        
        df = quote.history(start=start_date_str, end=end_date_str, interval='1D')
        df['high_low'] = df['high'] - df['low']
        df['high_close'] = np.abs(df['high'] - df['close'].shift(1))
        df['low_close'] = np.abs(df['low'] - df['close'].shift(1))
        df['true_range'] = df[['high_low', 'high_close', 'low_close']].max(axis=1)
        df['atr'] = df['true_range'].rolling(window=5).mean()
        df['atr_percentage'] = (df['atr'] / df['close']) * 100
        latest_atr = df[df['atr_percentage'].notna()].iloc[-1]['atr_percentage']
        atr_data.append({
                'symbol': sym,
                'atr_percentage': latest_atr
            })
        sleep(3)
    atr_df = pd.DataFrame(atr_data)
    if not existing_df.empty:
        
        updated_df = existing_df.merge(atr_df, on='symbol', how='left', suffixes=('', '_new'))
        
        if 'atr_percentage_new' in updated_df.columns:
            updated_df['atr_percentage'] = updated_df['atr_percentage_new'].fillna(updated_df.get('atr_percentage', np.nan))
            updated_df.drop('atr_percentage_new', axis=1, inplace=True)
            updated_df = updated_df.sort_values('atr_percentage', ascending=True).reset_index(drop=True)
            updated_df['rank_atr'] = range(1, len(updated_df) + 1)
    else:
        
        updated_df = atr_df
    updated_df.to_csv('Group4.csv', index=False)  
    return updated_df
calculate_atr_percentage_HNX30()