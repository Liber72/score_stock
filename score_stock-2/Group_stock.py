from vnstock import Listing, Quote, Vnstock, Company
import pandas as pd
from datetime import datetime, timedelta
from time import sleep
import schedule
import time

def Group_stock(group=''):
    end_date = datetime.now() 
    start_date = end_date - timedelta(days=0)
    start_date_str = start_date.strftime('%Y-%m-%d')
    end_date_str = end_date.strftime('%Y-%m-%d')
    print(f"Lấy dữ liệu từ {start_date_str} đến {end_date_str}")
    group_stocks = Listing(source='VCI').symbols_by_group(group).tolist()
    print(group_stocks)
    all_data = pd.DataFrame()
    for sym in group_stocks:
        quote = Quote(symbol=sym, source='VCI')
        print(f"Getting data for {sym}...")
        company = Company(source="vci", symbol=sym, random_agent=False, show_log=False)
        df_ov = company.overview()
        history_data = quote.history(start=start_date_str, end=end_date_str, interval='1D')
        
        issue_share = df_ov.get("issue_share").iloc[0]
        sleep(5)
        if not history_data.empty:
            selected_data = history_data[['time','open','high','low','close', 'volume']].copy()
            selected_data['issue_share'] = issue_share
            selected_data['symbol'] = sym
            selected_data['marketcap'] = selected_data['close'] * issue_share
            all_data = pd.concat([all_data, selected_data], ignore_index=True)
        
    if not all_data.empty:
        all_data['rank_volume'] = all_data['volume'].rank(method='min', ascending=False).astype(int)
        all_data['rank_marketcap'] = all_data['marketcap'].rank(method='min', ascending=False).astype(int)
        all_data['total_score'] = all_data['rank_volume'] + all_data['rank_marketcap']
        all_data = all_data.sort_values(by=['total_score', 'rank_volume'], ascending=[True, False])
    if not all_data.empty:
        all_data['rank_volume'] = all_data['volume'].rank(method='min', ascending=False).astype(int)
        all_data['rank_marketcap'] = all_data['marketcap'].rank(method='min', ascending=False).astype(int)
        all_data['total_score'] = all_data['rank_volume'] + all_data['rank_marketcap']

       
        all_data = all_data.sort_values(by=['total_score', 'volume'], ascending=[True, False])


        output_data = all_data[['symbol','time','open','high','low','close', 'volume', 'issue_share', 'marketcap', 
                               'rank_volume', 'rank_marketcap', 'total_score']]

        # Lưu toàn bộ nhóm VN100 vào 1 file duy nhất
        output_data.to_csv(f'VN100_Group.csv', index=False)
    all_data = all_data[['symbol', 'close', 'volume', 'issue_share', 'marketcap', 
                         'rank_volume', 'rank_marketcap', 'total_score']]
    all_data.to_csv(f'Top_{group}.csv', index=False)

    return all_data

