from pykalman import KalmanFilter
import pandas as pd
import datetime as dt
import pytz
import ccxt


def get_ohlcv_scalable(exch, sym, start, end, timeframe='1m'):
    df_list = []
    while start < end:
        df = pd.DataFrame(exch.fetch_ohlcv(symbol=sym, timeframe=timeframe,
                                           since=exch.parse8601(start.strftime("%Y-%m-%d%H:%M:%S"))))
        
        if not df.empty:
            df.columns = ['time', 'open', 'high', 'low', 'close', 'volume']
            df.time = pd.to_datetime(df.time, unit='ms')
            df.set_index('time', inplace=True)
            df_list.append(df)

        start += dt.timedelta(hours=4)

    df_final = pd.concat(df_list).loc[:end.strftime("%Y-%m-%d %H:%M:%S")]
    df_final.rename(columns={'volume': 'pub_volume'}, inplace=True)

    return df_final


def compute_strat_skew(sym, max_bp_skew=20):

    start = dt.datetime.now().astimezone(pytz.utc).replace(second=0, microsecond=0, tzinfo=None) - dt.timedelta(hours=12)
    end = dt.datetime.now().astimezone(pytz.utc).replace(second=0, microsecond=0, tzinfo=None)
    # BASE = 'sol'
    # QUOTE = 'usdt'
    exchange = 'okx'
    exch = getattr(ccxt, exchange)({
        'enableRateLimit': True})
    sym = sym

    df_public = get_ohlcv_scalable(exch, sym, start, end, timeframe='1m')
    df_public = df_public.drop_duplicates()

    # Kalman Filter Calculations
    kf = KalmanFilter(initial_state_mean=df_public['close'].iloc[0],
                    initial_state_covariance=1,
                    observation_covariance=5,
                    transition_covariance=1)
    
    state_means, _ = kf.filter(df_public['close'])
    kalman_series = pd.Series(state_means.flatten(), index=df_public.index)

    df_public['midprice'] = (df_public['high'] + df_public['low']) / 2
    bp_cap = max_bp_skew
    error = df_public['close'] - kalman_series
    error_zscore = (error - error.mean()) / error.std()
    df_public['error_zscore'] = error_zscore

    # Calculate adjustment as a ratio based on the error
    adjustment_ratio_series = 1 - error_zscore * (bp_cap / 10000)
    skew = adjustment_ratio_series[-1]

    print(F'Current Kalman strategy skew factor: {skew}')

    return df_public, skew
