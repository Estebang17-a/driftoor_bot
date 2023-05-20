from solana.keypair import Keypair
from solana.rpc.async_api import AsyncClient
from anchorpy import Provider, Wallet
import os
import json
import asyncio
from driftpy.constants.config import configs
from driftpy.types import *
from driftpy.constants.numeric_constants import BASE_PRECISION, PRICE_PRECISION
from driftpy.clearing_house import ClearingHouse
from driftpy.clearing_house_user import ClearingHouseUser
from borsh_construct.enum import _rust_enum
from dlob import fetch_dlob
import copy
import strategy

#collateral?

# Initialazing Driftpy Client
url = 'https://api.devnet.solana.com'
config = configs['devnet']
key_pair_file = os.environ.get('ANCHOR_WALLET')
with open(os.path.expanduser(key_pair_file), 'r') as f: secret = json.load(f) 
kp = Keypair.from_secret_key(bytes(secret))
wallet = Wallet(kp)
connection = AsyncClient(url)
provider = Provider(connection, wallet)
clearing_house = ClearingHouse.from_config(config, provider)
clearing_house_user = ClearingHouseUser(clearing_house, use_cache=True)



## Calculate orders
@_rust_enum
class PostOnlyParams:
    NONE = constructor()
    TRY_POST_ONLY = constructor()
    MUST_POST_ONLY = constructor()

async def send_orders(midprice, perp: True, market_index, base_asset_amount, spread, price_scaling, volume_scaling, order_number:5):

    market_type = MarketType.PERP() if perp else MarketType.SPOT()

    default_order_params = OrderParams(
        order_type=OrderType.LIMIT(),
        market_type=market_type,
        direction=PositionDirection.LONG(),
        user_order_id=0,
        base_asset_amount=0,
        price=int((midprice-1) * PRICE_PRECISION),
        market_index=market_index,
        reduce_only=False,
        post_only=PostOnlyParams.NONE(),
        immediate_or_cancel=False,
        trigger_price=0,
        trigger_condition=OrderTriggerCondition.ABOVE(),
        oracle_price_offset=0,
        auction_duration=None,
        max_ts=None,
        auction_start_price=None,
        auction_end_price=None,
    )


    bids = []
    asks = []
    bids_display = []
    asks_display = []
    for i in range(order_number):
        bid_order_params = copy.deepcopy(default_order_params)
        bid_order_params.direction = PositionDirection.LONG()
        bid_order_params.price = int(((midprice * (1 - (0.5 * spread / 10000))) - i * price_scaling / 1000) * PRICE_PRECISION)
        bid_order_params.base_asset_amount = int(base_asset_amount * BASE_PRECISION)
        bids.append(bid_order_params)
        bid_price = ((midprice * (1 - (0.5 * spread / 10000))) - i * price_scaling / 10000)
        bid_volume = base_asset_amount
        bids_display.append(f'bid price: {bid_price}, amount: {bid_volume}')

        ask_order_params = copy.deepcopy(default_order_params)
        ask_order_params.direction = PositionDirection.SHORT()
        ask_order_params.price = int(((midprice * (1 + (0.5 * spread / 10000))) + i * price_scaling / 10000) * PRICE_PRECISION)
        ask_order_params.base_asset_amount = int(base_asset_amount * BASE_PRECISION)
        asks.append(ask_order_params)
        ask_price = ((midprice * (1 + (0.5 * spread / 10000))) + i * price_scaling / 10000)
        ask_volume = base_asset_amount
        asks_display.append(f'ask price: {ask_price}, amount: {ask_volume}')

        base_asset_amount = base_asset_amount * volume_scaling

    orders = bids + asks

    ixs = await clearing_house.get_place_perp_orders_ix(orders)

    # await clearing_house.send_ixs(bids_ixs)
    await clearing_house.send_ixs(ixs)
    print('Current bids:')
    print("\n".join(bids_display), "\n")
    print('Current asks')
    print("\n".join(asks_display))


async def main():
    #Variables
    leverage_limit = 2
    spread = 100 # in bp
    price_scaling = 200 # in bp
    volume_scaling = 1.2
    order_number = 5
    marketIndex = 0
    max_strat_bp_skew = 20
    max_exposure_bp_skew = 100

    past_mdf = {}
    strat_skew_factor = 1
    exposure_skew_factor = 1

    await clearing_house_user.set_cache()
    ## Calculate midprice based on ob, repost only if x difference (pct) ???????????????

    while True:
        # Get DLOB, compute weighted mid price that is currently used for quoting
        mdf, long_orders, short_orders, tob_midprice, wlob_midprice = fetch_dlob(marketType='perp', marketIndex=marketIndex)

        if not past_mdf:
            past_mdf = mdf.to_dict()
        else:
            if mdf.to_dict() != past_mdf:
                past_mdf = mdf.to_dict() 

                await clearing_house_user.set_cache()

                # Compute Kalman Filter based skew and exposure based skew
                try:
                    strat_skew_factor = strategy.compute_strat_skew('SOL/USDT', max_strat_bp_skew)
                except:
                    strat_skew_factor = 1

                leverage = await clearing_house_user.get_leverage() / 10000
                print(f'Leverage: {leverage}')
                current_position = await clearing_house_user.get_user_position(marketIndex)
                print(f'Current position: {current_position.base_asset_amount}')

                if leverage > leverage_limit:
                    exposure_skew_factor = ((leverage - leverage_limit) / (leverage_limit) * max_exposure_bp_skew) / 10000

                    if current_position.base_asset_amount > 0:
                        exposure_skew_factor = 1 - exposure_skew_factor 
                    else:
                        exposure_skew_factor = 1 + exposure_skew_factor

                    print(f'Exposure skew factor: {exposure_skew_factor}')




            skewed_midprice = wlob_midprice * strat_skew_factor * exposure_skew_factor

            print(f'Weighted Midprice: {wlob_midprice}')
            print(f'Midprice after skew: {skewed_midprice} \n')

            await send_orders( 
                midprice=skewed_midprice,
                perp=True,
                market_index=marketIndex,
                base_asset_amount=50,
                spread=spread,
                price_scaling=price_scaling,
                volume_scaling=volume_scaling,
                order_number=order_number

                )  
            
        await asyncio.sleep(5)











if __name__ == '__main__':
    from dotenv import load_dotenv
    load_dotenv()
    import asyncio
    asyncio.run(main())

