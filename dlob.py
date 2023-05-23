import requests
import pandas as pd
import numpy as np

def fetch_dlob(marketIndex = 5, marketType = 'perp'):

    # Raw LOB
    url = "https://master.dlob.drift.trade/orders/json" # should I use the live OB? https://dlob.drift.trade/orders/json
    response = requests.get(url)
    dlob_data = response.json()

    # Snapshot processing
    market_to_oracle_map = pd.DataFrame(dlob_data['oracles']).set_index('marketIndex').to_dict()['price']

    df = pd.DataFrame([order['order'] for order in dlob_data['orders']])
    user = pd.DataFrame([order['user'] for order in dlob_data['orders']], columns=['user'])
    df = pd.concat([df, user],axis=1)
    df['oraclePrice'] = None
    df.loc[df.marketType=='perp', 'oraclePrice'] = df.loc[df.marketType=='perp', 'marketIndex'].apply(lambda x: market_to_oracle_map.get(x, 0))
    df.loc[df.marketType=='spot', 'oraclePrice'] = df.loc[df.marketType=='spot', 'marketIndex'].apply(lambda x: market_to_oracle_map.get(int(x)-1, 0))

    df1 = df[(df.orderType=='limit')].copy()

    df1.loc[((df1['price'].astype(np.int64)==0) & (df1['oraclePrice'].astype(np.int64)!=0)), 'price'] = df1['oraclePrice'].astype(np.int64) + df1['oraclePriceOffset'].astype(np.int64)

    for col in ['price', 'oraclePrice', 'oraclePriceOffset']:
        df1.loc[:, col] = df1.loc[:, col].astype(np.int64)
        df1.loc[:, col] /= 1e6
        
    for col in ['quoteAssetAmountFilled']:
        df1.loc[:, col] = df1.loc[:, col].astype(np.int64)
        df1.loc[:, col] /= 1e6 

    for col in ['baseAssetAmount', 'baseAssetAmountFilled']:
        df1.loc[:, col] = df1.loc[:, col].astype(np.int64)
        df1.loc[:, col] /= 1e9

    mdf = df1[((df1.marketType == marketType) & (df1.marketIndex == marketIndex))].copy()


    if len(mdf) > 0:

        mdf = mdf[['price', 'baseAssetAmount', 'direction', 'user', 'status', 'orderType', 'marketType', 'slot', 'orderId', 'userOrderId',
        'marketIndex',  'baseAssetAmountFilled',
        'quoteAssetAmountFilled',  'reduceOnly', 'triggerPrice',
        'triggerCondition', 'existingPositionDirection', 'postOnly',
        'immediateOrCancel', 'oraclePriceOffset', 'auctionDuration',
        'auctionStartPrice', 'auctionEndPrice', 'maxTs', 'oraclePrice']]
        mdf = mdf.sort_values('price').reset_index(drop=True)

        # TOB midprice and weighted LOB midprice
        long_orders = mdf[mdf['direction'] == 'long'][::-1].reset_index()
        short_orders = mdf[mdf['direction'] == 'short'].reset_index()

        try:
            tob_midprice = (long_orders.loc[0, 'price'] + short_orders.loc[0, 'price']) / 2
            long_orders['wMidprice'] = (long_orders['price'] * long_orders['baseAssetAmount']).cumsum()
            short_orders['wMidprice'] = (short_orders['price'] * short_orders['baseAssetAmount']).cumsum()

            volume_threshold = 100
            cumulative_bid_volume = 0
            cumulative_ask_volume = 0
            weighted_bid_sum = 0
            weighted_ask_sum = 0

            for _, row in long_orders.iterrows():
                bid_price = row['price']
                bid_volume = row['baseAssetAmount']
                cumulative_bid_volume += bid_volume
                weighted_bid_sum += bid_price * bid_volume

                if cumulative_bid_volume >= volume_threshold:
                    break

            for _, row in short_orders.iterrows():
                ask_price = row['price']
                ask_volume = row['baseAssetAmount']
                cumulative_ask_volume += ask_volume
                weighted_ask_sum += ask_price * ask_volume

                if cumulative_ask_volume >= volume_threshold:
                    break

            wlob_midprice = (weighted_bid_sum + weighted_ask_sum) / (cumulative_bid_volume + cumulative_ask_volume)

        except KeyError:
            tob_midprice = mdf.loc[0, 'oraclePrice']
            wlob_midprice = mdf.loc[0, 'oraclePrice']


        return mdf, long_orders, short_orders, tob_midprice, wlob_midprice

