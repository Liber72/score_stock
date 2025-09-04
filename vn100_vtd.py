import VN100_score  # Chạy script tạo groups trước
import HNX30_score  # Chạy script tạo HNX30 group
from vnstock import Quote
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from time import sleep

def calculate_volume_trading_deviation():
    """
    Tính chỉ số volume_trading_deviation cho các group VN100 và HNX30
    """
    
    print("Đang tính chỉ số volume_trading_deviation cho các group VN100 và HNX30...")
    
    # Đọc dữ liệu từ các file group đã tạo
    group_files = ['VN100_Group1.csv', 'VN100_Group2.csv', 'VN100_Group3.csv', 'Group4_Top_HNX30.csv']
    
    for i, file_name in enumerate(group_files, 1):
        print(f"\nXử lý {file_name}...")
        
        try:
            # Đọc dữ liệu group
            df_group = pd.read_csv(file_name)
            symbols = df_group['symbol'].tolist()
            
            # Tính ngày để lấy dữ liệu 5 ngày gần nhất
            end_date = datetime.now()
            start_date = end_date - timedelta(days=7)  # Lấy 7 ngày để đảm bảo có đủ 5 ngày giao dịch
            start_date_str = start_date.strftime('%Y-%m-%d')
            end_date_str = end_date.strftime('%Y-%m-%d')
            
            deviation_data = []
            
            for symbol in symbols:
                print(f"  Lấy dữ liệu cho {symbol}...")
                
                try:
                    # Lấy dữ liệu lịch sử 5 ngày
                    quote = Quote(symbol=symbol, source='VCI')
                    history_data = quote.history(start=start_date_str, end=end_date_str, interval='1D')
                    
                    if not history_data.empty and len(history_data) > 0:
                        # Lấy issue_share từ group data
                        issue_share = df_group[df_group['symbol'] == symbol]['issue_share'].iloc[0]
                        
                        # B1: Tính giá trị giao dịch của từng ngày val = close * volume
                        history_data['val'] = history_data['close'] * history_data['volume']
                        
                        # Lấy 5 ngày gần nhất
                        recent_data = history_data.tail(5)
                        
                        if len(recent_data) >= 2:  # Cần ít nhất 2 ngày để tính độ lệch chuẩn
                            # B2: Tính trung bình tuần của val
                            weekly_average_val = recent_data['val'].mean()
                            
                            # B3: Tính độ lệch chuẩn sd_val
                            sd_val = np.std(recent_data['val'], ddof=1)  # Sử dụng sample standard deviation
                            
                            # B4: Tính cv_val = sd_val / weekly_average_val
                            if weekly_average_val > 0:
                                cv_val = sd_val / weekly_average_val
                            else:
                                cv_val = float('inf')  # Tránh chia cho 0
                            
                            deviation_data.append({
                                'symbol': symbol,
                                'issue_share': issue_share,
                                'weekly_average_val': weekly_average_val,
                                'sd_val': sd_val,
                                'cv_val': cv_val,
                                'close': df_group[df_group['symbol'] == symbol]['close'].iloc[0],
                                'volume': df_group[df_group['symbol'] == symbol]['volume'].iloc[0],
                                'marketcap': df_group[df_group['symbol'] == symbol]['marketcap'].iloc[0],
                                'total_score': df_group[df_group['symbol'] == symbol]['total_score'].iloc[0]
                            })
                    
                    sleep(2)  # Giảm thời gian chờ
                    
                except Exception as e:
                    print(f"    Lỗi khi xử lý {symbol}: {e}")
                    continue
            
            if deviation_data:
                # Tạo DataFrame từ dữ liệu deviation
                df_deviation = pd.DataFrame(deviation_data)
                
                # B5: Xếp hạng cv_val (cv_val càng thấp thì xếp hạng càng cao - hạng 1 là cao nhất)
                df_deviation['cv_val_rank'] = df_deviation['cv_val'].rank(method='min', ascending=True).astype(int)
                
                # B6: Xếp hạng weekly_average_val (weekly_average_val càng cao thì xếp hạng càng cao - hạng 1 là cao nhất)
                df_deviation['weekly_val_rank'] = df_deviation['weekly_average_val'].rank(method='min', ascending=False).astype(int)
                
                # B7: Tính điểm volume_trading_deviation
                df_deviation['combined_score'] = df_deviation['cv_val_rank'] + df_deviation['weekly_val_rank']
                
                # Xếp hạng điểm combined_score (thấp hơn = tốt hơn = hạng cao hơn)
                # Nếu bằng điểm thì mã có cv_val_rank cao hơn (thấp hơn về giá trị cv_val) sẽ xếp trên
                df_deviation = df_deviation.sort_values(['combined_score', 'cv_val_rank'], ascending=[True, True])
                df_deviation['volume_trading_deviation_rank'] = range(1, len(df_deviation) + 1)
                
                # Chọn các cột cần thiết
                output_columns = ['symbol', 'close', 'volume', 'issue_share', 'marketcap', 
                                'weekly_average_val', 'sd_val', 'cv_val', 'cv_val_rank', 
                                'weekly_val_rank', 'combined_score', 'volume_trading_deviation_rank', 'total_score']
                df_output = df_deviation[output_columns]
                
                # B8: Xuất file CSV từng nhóm
                if 'HNX30' in file_name:
                    output_file = f'Group4_vtd.csv'
                else:
                    output_file = f'Group{i}_vtd.csv'

                df_output.to_csv(output_file, index=False)
                
                print(f"  Đã xuất kết quả ra {output_file}")

                top5 = df_output.head()[['symbol', 'cv_val', 'weekly_average_val', 'volume_trading_deviation_rank']]
                print(top5.to_string(index=False))
            
        except Exception as e:
            print(f"Lỗi khi xử lý {file_name}: {e}")
            continue
    
    print("\nHoàn thành tính toán chỉ số volume_trading_deviation cho VN100 và HNX30!")

if __name__ == "__main__":
    calculate_volume_trading_deviation()
