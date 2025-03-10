import pandas as pd
from sklearn.linear_model import LinearRegression

NY_TZ = "America/New_York"

def compute_regression_constants():
    """
    Computes regression constants A and B for modeling the ratio (SPY/SPX)
    as a function of timestamp (converted to a numeric value using the ordinal date).

    The function:
      - Loads SPY and SPX data from parquet files.
      - Converts their timestamps to NYC tz-aware datetimes.
      - Filters both datasets to the most recent 2 years based on the common maximum date.
      - Merges the datasets on date (ignoring the time component).
      - Computes the ratio = SPY close / SPX close.
      - Converts the date to a numeric representation (ordinal).
      - Fits a linear regression model: ratio = A + B * (date_numeric).

    Returns:
      tuple: (A, B) where A is the intercept and B is the slope.
    """
    # Load the data
    spy = pd.read_parquet("/fin/us_stocks_sip/day_aggs_by_ticker/SPY.parquet")
    spx = pd.read_parquet("/fin/us_indices/day_aggs_by_ticker/I:SPX.parquet")
    
    # Convert index to datetime and ensure tz-awareness in NYC
    spy.index = pd.to_datetime(spy.index)
    spx.index = pd.to_datetime(spx.index)
    
    if spy.index.tz is None:
        spy.index = spy.index.tz_localize(NY_TZ)
    else:
        spy.index = spy.index.tz_convert(NY_TZ)
        
    if spx.index.tz is None:
        spx.index = spx.index.tz_localize(NY_TZ)
    else:
        spx.index = spx.index.tz_convert(NY_TZ)
    
    # Determine common maximum date and filter to the most recent 2 years
    common_max_date = min(spy.index.max(), spx.index.max())
    start_date = common_max_date - pd.DateOffset(years=2)
    
    spy_recent = spy[spy.index >= start_date].copy()
    spx_recent = spx[spx.index >= start_date].copy()
    
    # Create a 'date' column (ignoring time) for merging
    spy_recent['date'] = spy_recent.index.date
    spx_recent['date'] = spx_recent.index.date
    
    # Merge on date to get common trading days
    merged = pd.merge(spy_recent, spx_recent, on='date', how='inner', suffixes=('_spy', '_spx'))
    
    # Compute the ratio SPY/SPX
    merged['ratio'] = merged['close_spy'] / merged['close_spx']
    
    # Convert the date to a numeric value using ordinal (number of days)
    merged['date_numeric'] = pd.to_datetime(merged['date']).apply(lambda dt: dt.toordinal())
    
    # Set up and fit the regression: ratio = A + B * date_numeric
    X = merged[['date_numeric']]
    y = merged['ratio']
    
    model = LinearRegression()
    model.fit(X, y)
    
    return model.intercept_, model.coef_[0]

# Precompute regression constants for the ratio model
REG_INTERCEPT, REG_SLOPE = compute_regression_constants()

print("Regression constants for ratio SPY/SPX = A + B * (date ordinal):")
print("A (intercept):", REG_INTERCEPT)
print("B (slope):", REG_SLOPE)

def predict_spy_from_spx(timestamp: str, spx_close: float) -> float:
    """
    Predicts the SPY closing price for a given timestamp and SPX closing price.

    The regression model is:
      ratio = A + B * (date_numeric)
      SPY = ratio * SPX

    Parameters:
      timestamp (str): The timestamp (or date string) for which to predict the ratio.
      spx_close (float): The SPX closing price.

    Returns:
      float: The predicted SPY closing price.
    """
    # Convert the provided timestamp to a date and then to its ordinal representation
    ts_date = pd.to_datetime(timestamp).date()
    ts_numeric = pd.to_datetime(ts_date).toordinal()
    
    # Compute the predicted ratio using the regression model
    predicted_ratio = REG_INTERCEPT + REG_SLOPE * ts_numeric
    
    # Use the predicted ratio to get the predicted SPY price
    predicted_spy = predicted_ratio * spx_close
    return predicted_spy

if __name__ == "__main__":
    example_timestamp = "2024-05-15"
    example_spx_close = 4500.0  # Replace with an actual SPX closing price as needed
    predicted_spy = predict_spy_from_spx(example_timestamp, example_spx_close)
    print(f"For timestamp {example_timestamp} and SPX price {example_spx_close}, the predicted SPY price is {predicted_spy}")

