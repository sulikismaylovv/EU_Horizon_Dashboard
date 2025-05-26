# src/models/forecast.py

import pandas as pd
from prophet import Prophet
from typing import List

def forecast_series(
    df: pd.DataFrame,
    freq: str,
    horizon: int,
    interval_width: float = 0.80
) -> pd.DataFrame:
    """
    Fit Prophet on a univariate time series and return forecasts.

    Parameters:
    - df: DataFrame with columns ['ds','y']
    - freq: 'Y' for annual or 'Q' for quarterly
    - horizon: number of periods ahead (years or quarters)
    - interval_width: width of the prediction interval (0 < w < 1)

    Returns:
    - DataFrame with ['ds','yhat','yhat_lower','yhat_upper']
    """
    m = Prophet(
        interval_width=interval_width,
        yearly_seasonality=(freq=='Y'),
        weekly_seasonality=False,
        daily_seasonality=False
    )
    m.fit(df)

    periods = horizon if freq == 'Y' else horizon * 4
    future = m.make_future_dataframe(periods=periods, freq=freq)
    fc = m.predict(future)
    return fc[['ds','yhat','yhat_lower','yhat_upper']]
