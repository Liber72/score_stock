import pandas as pd
import numpy as np
from vnstock import Vnstock
from datetime import datetime, timedelta
from time import sleep
import glob

def calc_amihud(symbol, start, end):
    try:
        stock = Vnstock().stock(symbol=symbol, source='VCI')
        df = stock.quote.history(start=start, end=end, interval='1D')

        if df.empty or 'close' not in df or 'volume' not in df:
            return np.nan

        df = df[['time', 'close', 'volume']].copy()
        df.sort_values('time', inplace=True)
        df.reset_index(drop=True, inplace=True)

        df['return'] = np.log(df['close'] / df['close'].shift(1))
        df['ILLIQ_t'] = abs(df['return']) / (df['close'] * df['volume'])

        mean_ILLIQ_t = df['ILLIQ_t'].tail(5).mean()
        return mean_ILLIQ_t
    except Exception as e:
        print(f"Lỗi khi xử lý {symbol}: {e}")
        return np.nan
    



def calc_all_groups_amihud():

    group_files = ['VN100_Group.csv', 'HNX30_Group.csv']
    group_names = ['VN100', 'HNX30']
    
    for file_name, group_name in zip(group_files, group_names):
        print(f"\nĐang xử lý {file_name}...")
        
        try:
            df_group = pd.read_csv(file_name)
            symbols = df_group['symbol'].tolist()
            
            end_date = datetime.now()
            start_date = end_date - timedelta(days=12)
            start_date_str = start_date.strftime('%Y-%m-%d')
            end_date_str = end_date.strftime('%Y-%m-%d')
            
            amihud_list = []
            
            for  symbol in symbols:
                
                illiq = calc_amihud(symbol, start=start_date_str, end=end_date_str)
                
                amihud_list.append({'symbol': symbol, 'mean_ILLIQ_t': illiq})
                
                sleep(3)  
                
            df_amihud = pd.DataFrame(amihud_list)

            df_group = df_group.merge(df_amihud, on="symbol", how="left")

            df_group['rank_amihud'] = df_group['mean_ILLIQ_t'].rank(ascending=True, method='dense').astype('Int64')

            output_file = f'{group_name}_amihud.csv'
            df_group.to_csv(output_file, index=False)
            print(f"Đã lưu kết quả vào {output_file}")
        
        except Exception as e:
            print(f"Lỗi khi xử lý {file_name}: {e}")


calc_all_groups_amihud()



















