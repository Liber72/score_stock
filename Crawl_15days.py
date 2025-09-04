from vnstock import Listing, Quote, Vnstock, Company
import pandas as pd
from datetime import datetime, timedelta
from time import sleep


def Group_stock():
    end_date = datetime.now() 
    start_date = end_date - timedelta(days=15)
    start_date_str = start_date.strftime('%Y-%m-%d')
    end_date_str = end_date.strftime('%Y-%m-%d')
    print(f"Lấy dữ liệu từ {start_date_str} đến {end_date_str}")
    group_stocks = Listing(source='VCI').symbols_by_group('VN100').tolist()
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

        
        group1 = output_data.iloc[:30]  
        group2 = output_data.iloc[30:60]  
        group3 = output_data.iloc[60:]  

        group1.to_csv(f'Group1.csv', index=False)
        group2.to_csv(f'Group2.csv', index=False)
        group3.to_csv(f'Group3.csv', index=False)

        output_data.to_csv(f'Top_VN100_All.csv', index=False)
    all_data = all_data[['symbol', 'close', 'volume', 'issue_share', 'marketcap', 
                         'rank_volume', 'rank_marketcap', 'total_score']]
    all_data.to_csv(f'Top_VN100.csv', index=False)

    return all_data

# Ví dụ sử dụng, Muốn chạy cho VN100 thì bỏ comment dòng dưới
# Group_stock('VN100')
