import pandas as pd
from sklearn.linear_model import LinearRegression

NY_TZ = "America/New_York"

def compute_regression_constants():
    """
    Computes the regression constants A (intercept) and B (coefficient) for predicting SPX close price from SPY close price.
    
    The function:
      - Loads the SPY and SPX parquet files.
      - Converts the indices to NYC tz-aware datetimes.
      - Filters both datasets to the most recent 2 years (based on the common maximum date).
      - Merges the datasets on the date.
      - Fits a linear regression model with SPY's 'close' as predictor and SPX's 'close' as target.
    
    Returns:
      tuple: (A, B) where A is the intercept and B is the slope of the regression.
    """
    # Load data
    spy = pd.read_parquet("/fin/us_stocks_sip/day_aggs_by_ticker/SPY.parquet")
    spx = pd.read_parquet("/fin/us_indices/day_aggs_by_ticker/I:SPX.parquet")
    
    # Convert index to datetime and ensure tz-aware in NYC
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
    
    # Determine the common maximum date and the start date two years earlier
    common_max_date = min(spy.index.max(), spx.index.max())
    start_date = common_max_date - pd.DateOffset(years=1)
    
    # Filter the datasets to only include data from the most recent 2 years
    spy_recent = spy[spy.index >= start_date].copy()
    spx_recent = spx[spx.index >= start_date].copy()
    
    # Merge on date (ignoring the time)
    spy_recent['date'] = spy_recent.index.date
    spx_recent['date'] = spx_recent.index.date
    merged = pd.merge(spy_recent, spx_recent, on='date', how='inner', suffixes=('_spy', '_spx'))
    
    # Set up regression: predict SPX close from SPY close
    X = merged[['close_spy']]
    y = merged['close_spx']
    
    model = LinearRegression()
    model.fit(X, y)
    
    return model.intercept_, model.coef_[0]

# Compute the regression constants once
INTERCEPT, COEFFICIENT = compute_regression_constants()

print("Regression constants:")
print("A (intercept):", INTERCEPT)
print("B (coefficient):", COEFFICIENT)

def predict_spx_from_spy_value(spy_close: float) -> float:
    """
    Predicts the SPX closing price given a SPY closing price using precomputed regression constants.
    
    Formula:
      SPX = A + B * SPY
      
    Parameters:
      spy_close (float): The SPY closing price.
      
    Returns:
      float: The predicted SPX closing price.
    """
    return INTERCEPT + COEFFICIENT * spy_close

# Example usage:
if __name__ == "__main__":
    example_spy_close = 350.0  # Replace with an actual SPY price as needed
    predicted_spx = predict_spx_from_spy_value(example_spy_close)
    print(f"For a SPY price of {example_spy_close}, the predicted SPX price is {predicted_spx}")

