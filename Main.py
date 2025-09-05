import Group_stock, HNX30_score,ATR,Spread,VTD,vn100_trv,Amihud
import pandas as pd

Group_stock.Group_stock(group='VN100')#Group123
HNX30_score.calculate_hnx30_score()#group4
ATR.calculate_atr_all_groups()
Spread.run_all_groups_analysis()
VTD.calculate_volume_trading_deviation()
vn100_trv.calculate_weekly_turnover()
Amihud.calc_all_groups_amihud()

i=1
final_results = {}
while i<5:

    pf_atr= pd.read_csv(f'Group{i}_ATR.csv')
    pf_spread= pd.read_csv(f'Group{i}_SR.csv')
    pf_vtd= pd.read_csv(f'Group{i}_vtd.csv')
    pf_turnover= pd.read_csv(f'Group{i}_Turnover.csv')
    pf_amihud= pd.read_csv(f'Group{i}_amihud.csv')

    atr_rank = pf_atr[['symbol', 'rank_atr']].rename(columns={'rank_atr': 'atr_rank'})
    spread_rank = pf_spread[['symbol', 'rank_spread']].rename(columns={'rank_spread': 'spread_rank'})
    vtd_rank = pf_vtd[['symbol', 'volume_trading_deviation_rank']].rename(columns={'volume_trading_deviation_rank': 'vtd_rank'})
    turnover_rank = pf_turnover[['symbol', 'turnover_rank']]
    amihud_rank = pf_amihud[['symbol', 'rank_amihud']].rename(columns={'rank_amihud': 'amihud_rank'})
    
    # Merge tất cả các dataframe theo symbol
    merged_df = atr_rank
    merged_df = merged_df.merge(spread_rank, on='symbol', how='outer')
    merged_df = merged_df.merge(vtd_rank, on='symbol', how='outer')
    merged_df = merged_df.merge(turnover_rank, on='symbol', how='outer')
    merged_df = merged_df.merge(amihud_rank, on='symbol', how='outer')
    rank_columns = ['spread_rank', 'vtd_rank', 'turnover_rank', 'amihud_rank']
    merged_df['combined_4_indicators'] = merged_df[rank_columns].sum(axis=1)
    merged_df['combined_rank'] = merged_df['combined_4_indicators'].rank(method='min', ascending=True)
    merged_df['total_rank'] = merged_df[rank_columns].sum(axis=1)
    merged_df['final_score'] = merged_df['combined_rank'] + merged_df['atr_rank']
    
    # Xếp hạng cuối cùng (từ thấp đến cao)
    merged_df['final_rank'] = merged_df['final_score'].rank(method='min', ascending=True)
    merged_df = merged_df.sort_values('final_rank')
    #bỏ cột trung gian
    merged_df = merged_df.drop('combined_4_indicators', axis=1)
    # Lưu kết quả vào dictionary
    final_results[f'Group{i}'] = merged_df
    merged_df.to_csv(f'Group{i}_Final.csv', index=False)

    i += 1