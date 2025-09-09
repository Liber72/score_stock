import pandas as pd
import numpy as np
import time
from datetime import datetime, timedelta
from vnstock import Quote, Listing


def fetch_historical_data(symbols: list, start_date_str: str, end_date_str: str) -> pd.DataFrame:
    """Tải dữ liệu lịch sử (bao gồm high, low) cho một danh sách mã cổ phiếu."""
    all_data_list = []
    for sym in symbols:
        print(f"--- Tải dữ liệu lịch sử cho: {sym} ---")
        try:
            df = Quote(symbol=sym).history(start=start_date_str, end=end_date_str, interval='1D')
            if not df.empty and 'high' in df.columns and 'low' in df.columns:
                df['symbol'] = sym
                all_data_list.append(df)
            else:
                print(f"Không có đủ dữ liệu 'high'/'low' cho mã {sym}.")
            time.sleep(2)
        except Exception as e:
            print(f"Lỗi khi tải dữ liệu lịch sử cho {sym}: {e}")
    return pd.concat(all_data_list, ignore_index=True) if all_data_list else pd.DataFrame()


def calculate_corwin_schultz_spread(df: pd.DataFrame) -> pd.DataFrame:
    """Tính toán Corwin-Schultz Spread cho DataFrame của một mã chứng khoán."""
    if len(df) < 2: return pd.DataFrame()
    df = df.sort_values(by='time').reset_index(drop=True)
    df[['high', 'low']] = df[['high', 'low']].apply(pd.to_numeric)
    df['log_hl_sq'] = (np.log(df['high'] / df['low'])) ** 2
    df['beta_t'] = df['log_hl_sq'] + df['log_hl_sq'].shift(-1)
    max_high_2day = df['high'].rolling(window=2).max()
    min_low_2day = df['low'].rolling(window=2).min()
    df['gamma_t'] = (np.log(max_high_2day.shift(-1) / min_low_2day.shift(-1))) ** 2
    df['alpha_t'] = np.sqrt(np.maximum(df['gamma_t'] - df['beta_t'] / 2, 0))
    df['spread_cs'] = (2 * (np.exp(df['alpha_t']) - 1)) / (1 + np.exp(df['alpha_t']))
    return df.dropna(subset=['spread_cs'])


def run_spread_analysis(symbols: list, group_label: str, days: int):
   
    print(f"\n--- Bắt đầu xử lý cho {group_label} ---")
    if not symbols:
        print(f"Danh sách mã trống, không có gì để phân tích.")
        return

    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)
    start_date_str, end_date_str = start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')
    print(f"Phân tích dữ liệu từ {start_date_str} đến {end_date_str}")

    full_df = fetch_historical_data(symbols, start_date_str, end_date_str)
    if full_df.empty:
        print(f"Không tải được dữ liệu lịch sử cho nhóm.")
        return

    processed_df = full_df.groupby('symbol').apply(calculate_corwin_schultz_spread).reset_index(drop=True)
    if processed_df.empty:
        print(f"Không thể tính toán spread cho nhóm.")
        return

    avg_spread = processed_df.groupby('symbol')['spread_cs'].mean().reset_index(name='average_spread')
    avg_spread = avg_spread.sort_values(by='average_spread', ascending=True).reset_index(drop=True)
    avg_spread['rank_spread'] = avg_spread['average_spread'].rank(method='min').astype(int)

    latest_data = processed_df.sort_values('time').groupby('symbol').tail(1)
    final_df = pd.merge(avg_spread, latest_data, on='symbol', how='left')

    output_cols = ['rank_spread', 'symbol', 'average_spread', 'close', 'open', 'high', 'low', 'volume']
    final_df = final_df.reindex(columns=output_cols)
    
    output_filename = f"{group_label}_SR.csv"
    final_df.to_csv(output_filename, index=False, encoding='utf-8-sig')
    

def run_all_groups_analysis():
    
    vn100_symbols = pd.read_csv('VN100_Group.csv')['symbol'].tolist()
    hnx30_symbols = pd.read_csv('HNX30_Group.csv')['symbol'].tolist()
    
    run_spread_analysis(
        symbols=vn100_symbols,
        group_label="VN100",
        days=15,
    )
    run_spread_analysis(
        symbols=hnx30_symbols,
        group_label="HNX30",
        days=15,
    )


if __name__ == '__main__':
    run_all_groups_analysis()