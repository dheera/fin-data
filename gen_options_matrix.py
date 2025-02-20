#!/usr/bin/env python3
#!/usr/bin/env python3

import argparse
import os
import glob
import pandas as pd

# NYSE trading hours (Eastern Time)
TRADING_HOURS = pd.date_range("09:30:00", "16:00:00", freq="min").time  # Every minute from 09:30 to 16:00

def main():
    parser = argparse.ArgumentParser(
        description="Aggregate minute option data into call/put volumes and compute volume-weighted EMA10 vwas for calls and puts."
    )
    parser.add_argument("input_dir", help="Path to the input directory of daily Parquet files.")
    parser.add_argument("output_dir", help="Path to the output directory for aggregated Parquet files.")
    args = parser.parse_args()

    # Ensure the output directory exists
    os.makedirs(args.output_dir, exist_ok=True)

    # Get all Parquet files in the input directory
    file_paths = sorted(glob.glob(os.path.join(args.input_dir, "*.parquet")), reverse=True)

    for file_path in file_paths:
        out_name = os.path.basename(file_path)
        out_path = os.path.join(args.output_dir, out_name)

        if os.path.exists(out_path):
            print(f"{out_path} exists, skipping")
            continue

        # Read the Parquet file into a DataFrame
        df = pd.read_parquet(file_path)
        df.reset_index(inplace=True)

        # --------------------------------------------------
        # 1) Identify top-1024 underlyings by total volume
        # --------------------------------------------------
        total_volume_by_und = df.groupby('underlying')['volume'].sum()
        top_und = total_volume_by_und.nlargest(1024).index

        # Filter df to only those underlyings
        df = df[df['underlying'].isin(top_und)]

        # --------------------------------------------------
        # 2) Aggregate minute-level volume for calls and puts (for volume pivot table)
        # --------------------------------------------------
        grouped = df.groupby(['window_start', 'underlying', 'type'], as_index=False)['volume'].sum()
        grouped['volume'] = grouped['volume'].fillna(0)
        pivoted = grouped.pivot_table(
            index='window_start',
            columns=['underlying', 'type'],
            values='volume',
            fill_value=0
        )
        # Guarantee that all underlyings have both types
        all_underlying_types = pd.MultiIndex.from_product([top_und, ['C', 'P']], names=['underlying', 'type'])
        pivoted = pivoted.reindex(columns=all_underlying_types, fill_value=0)
        pivoted.rename(columns={'C': 'C_volume', 'P': 'P_volume'}, level=1, inplace=True)

        # --------------------------------------------------
        # 3) For volume-weighted EMA, aggregate the minute-level data for calls and puts:
        #    Calculate for each minute the sum(volume) and sum(volume * strike)
        # --------------------------------------------------
        # For calls:
        df_calls = df[df['type'] == 'C'].copy()
        df_calls['vol_strike'] = df_calls['volume'] * df_calls['strike']
        agg_calls = df_calls[['window_start', 'underlying', 'volume', 'vol_strike']].groupby(
            ['window_start', 'underlying']
        ).agg({
            'volume': 'sum',
            'vol_strike': 'sum'
        }).reset_index()
        calls_vol = agg_calls.pivot(index='window_start', columns='underlying', values='volume')
        calls_vol_strike = agg_calls.pivot(index='window_start', columns='underlying', values='vol_strike')

        # For puts:
        df_puts = df[df['type'] == 'P'].copy()
        df_puts['vol_strike'] = df_puts['volume'] * df_puts['strike']
        agg_puts = df_puts[['window_start', 'underlying', 'volume', 'vol_strike']].groupby(
            ['window_start', 'underlying']
        ).agg({
            'volume': 'sum',
            'vol_strike': 'sum'
        }).reset_index()
        puts_vol = agg_puts.pivot(index='window_start', columns='underlying', values='volume')
        puts_vol_strike = agg_puts.pivot(index='window_start', columns='underlying', values='vol_strike')

        # --------------------------------------------------
        # 4) Reindex to full set of NYSE trading minutes
        # --------------------------------------------------
        # Extract trading_date from the existing pivot (assuming all timestamps are from the same day)
        trading_date = pivoted.index[0].date()
        full_index = pd.date_range(
            start=f"{trading_date} 09:30:00", 
            end=f"{trading_date} 16:00:00", 
            freq="min", 
            tz=pivoted.index.tz
        )

        pivoted = pivoted.reindex(full_index, fill_value=0)
        calls_vol = calls_vol.reindex(full_index, fill_value=0)
        calls_vol_strike = calls_vol_strike.reindex(full_index, fill_value=0)
        puts_vol = puts_vol.reindex(full_index, fill_value=0)
        puts_vol_strike = puts_vol_strike.reindex(full_index, fill_value=0)

        # --------------------------------------------------
        # 5) Compute the volume-weighted EMA (span=10) for calls and puts
        # --------------------------------------------------
        # For calls: compute EMA on both aggregated vol_strike and volume, then divide.
        calls_ema_numerator = calls_vol_strike.ewm(span=10, adjust=False).mean()
        calls_ema_denom = calls_vol.ewm(span=10, adjust=False).mean()
        C_vwas_ema10 = calls_ema_numerator.div(calls_ema_denom).fillna(0)

        # For puts:
        puts_ema_numerator = puts_vol_strike.ewm(span=10, adjust=False).mean()
        puts_ema_denom = puts_vol.ewm(span=10, adjust=False).mean()
        P_vwas_ema10 = puts_ema_numerator.div(puts_ema_denom).fillna(0)

        # Rename columns to indicate the indicator names
        C_vwas_ema10.columns = pd.MultiIndex.from_product([C_vwas_ema10.columns, ['C_vwas_ema10']])
        P_vwas_ema10.columns = pd.MultiIndex.from_product([P_vwas_ema10.columns, ['P_vwas_ema10']])

        # --------------------------------------------------
        # 6) Concatenate the volume pivot with the EMA indicators and write to Parquet
        # --------------------------------------------------
        final_df = pd.concat([pivoted, C_vwas_ema10, P_vwas_ema10], axis=1)

        # Convert volume columns to int32 (EMA columns remain float)
        volume_cols = [col for col in final_df.columns if col[1] in ['C_volume', 'P_volume']]
        final_df = final_df.astype({col: 'int32' for col in volume_cols})

        final_df.to_parquet(out_path)
        print(f"Processed {file_path} -> {out_path}")

if __name__ == "__main__":
    main()

