
from vnstock import Quote
import pandas as pd
from datetime import datetime, timedelta
from time import sleep

def calculate_weekly_turnover():
   
    
    # Đọc dữ liệu từ các file group đã tạo
    group_files = ['VN100_Group.csv', 'HNX30_Group.csv']
    group_names = ['VN100', 'HNX30']
    
    for file_name, group_name in zip(group_files, group_names):
        print(f"\nXử lý {file_name}...")
        
        try:
            # Đọc dữ liệu group
            df_group = pd.read_csv(file_name)
            symbols = df_group['symbol'].tolist()
            
            
            end_date = datetime.now()
            start_date = end_date - timedelta(days=12)  # Lấy 12 ngày để đảm bảo có đủ 5 ngày giao dịch
            start_date_str = start_date.strftime('%Y-%m-%d')
            end_date_str = end_date.strftime('%Y-%m-%d')
            
            turnover_data = []
            
            for symbol in symbols:
                print(f"  Lấy dữ liệu cho {symbol}...")
                
                try:
                    # Lấy dữ liệu lịch sử 5 ngày
                    quote = Quote(symbol=symbol, source='VCI')
                    history_data = quote.history(start=start_date_str, end=end_date_str, interval='1D')
                    
                    if not history_data.empty and len(history_data) > 0:
                        
                        issue_share = df_group[df_group['symbol'] == symbol]['issue_share'].iloc[0]
                        
                        # Tính turnover hàng ngày = volume / issue_share
                        history_data['turnover_daily'] = history_data['volume'] / issue_share
                        
                        
                        recent_data = history_data.tail(5)
                        
                        
                        weekly_avg_turnover = recent_data['turnover_daily'].mean()
                        
                        turnover_data.append({
                            'symbol': symbol,
                            'issue_share': issue_share,
                            'weekly_avg_turnover': weekly_avg_turnover,
                            'close': df_group[df_group['symbol'] == symbol]['close'].iloc[0],
                            'volume': df_group[df_group['symbol'] == symbol]['volume'].iloc[0],
                            'marketcap': df_group[df_group['symbol'] == symbol]['marketcap'].iloc[0],
                            'total_score': df_group[df_group['symbol'] == symbol]['total_score'].iloc[0]
                        })
                    
                    sleep(2)  
                    
                except Exception as e:
                    print(f"    Lỗi khi xử lý {symbol}: {e}")
                    continue
            
            if turnover_data:
                
                df_turnover = pd.DataFrame(turnover_data)
                
                
                df_turnover['turnover_rank'] = df_turnover['weekly_avg_turnover'].rank(method='min', ascending=False).astype(int)
                
                # Sắp xếp theo hạng turnover
                df_turnover = df_turnover.sort_values('turnover_rank')
                
                # Chọn các cột cần thiết
                output_columns = ['symbol', 'close', 'volume', 'issue_share', 'marketcap', 
                                'weekly_avg_turnover', 'turnover_rank', 'total_score']
                df_output = df_turnover[output_columns]
                
                # Xuất file kết quả
                output_file = f'{group_name}_Turnover.csv'
                df_output.to_csv(output_file, index=False)
                
                print(f"  Đã xuất kết quả ra {output_file}")
                
            
        except Exception as e:
            print(f"Lỗi khi xử lý {file_name}: {e}")
            continue
    

if __name__ == "__main__":
    calculate_weekly_turnover()
