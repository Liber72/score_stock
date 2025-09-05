from vnstock import Listing, Quote, Vnstock, Company
import pandas as pd
from datetime import datetime, timedelta
import numpy as np
from time import sleep
def calculate_atr(group_file, output_file):
    end_date = datetime.now() 
    start_date = end_date - timedelta(days=10)
    start_date_str = start_date.strftime('%Y-%m-%d')
    end_date_str = end_date.strftime('%Y-%m-%d')
    group_df = pd.read_csv(group_file)
    group_stocks = group_df['symbol'].tolist()
    
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
    if not atr_df.empty:
        
        atr_df = atr_df.sort_values('atr_percentage', ascending=True).reset_index(drop=True)
        atr_df['rank_atr'] = range(1, len(atr_df) + 1)
        atr_df.to_csv(output_file, index=False)
    return atr_df 

def calculate_atr_all_groups():

    groups = [
        ('Group1.csv', 'Group1_ATR.csv'),
        ('Group2.csv', 'Group2_ATR.csv'), 
        ('Group3.csv', 'Group3_ATR.csv'),
        ('Group4.csv', 'Group4_ATR.csv')
    ]
    
    results = {}
    
    for group_file, output_file in groups:
        print(f"\nBắt đầu tính ATR cho {group_file}...")
        result = calculate_atr(group_file, output_file)
        results[group_file] = result
        
    return results
