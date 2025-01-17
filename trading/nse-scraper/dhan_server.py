
# from functools import lru_cache
# import time
# import traceback
# from dhanhq import marketfeed
# from datetime import datetime, timedelta, timezone
# import asyncio
# import nest_asyncio
# import socketio
# from aiohttp import web
# import json
# import websockets
# from config import settings
# from pymongo import MongoClient
# import logging
# import os



# # Apply the nest_asyncio patch
# nest_asyncio.apply()
# # Read environment variables
# client_id = settings.DHAN_CLIENT_ID
# access_token = settings.DHAN_ACCESS_TOKEN

# # prev_closing_prices_data = {}

# version = "v2"  # Mention Version and set to latest version 'v2'

# # MongoDB setup
# mongo_client = MongoClient(settings.MONGO_URI)
# db = mongo_client[settings.MONGO_DATABASE]
# collection = db[settings.MONGO_COLLECTION]
# instruments_collection = db["ticker_info"]


# # Create a Socket.IO server
# sio = socketio.AsyncServer(async_mode='aiohttp',
#                            ping_interval=25,  # Default is 25 seconds
#                            ping_timeout=60,   # Default is 60 seconds
#                            )
# app = web.Application()
# sio.attach(app)


# async def index(request):
#     return web.FileResponse('public/index.html')

# # Setup routes for static files
# app.router.add_get('/', index)
# # app.router.add_static('/', path='public', name='public')
# app.router.add_static('/', path='public', name='static')


# @lru_cache(maxsize=128)
# def get_instruments_data():
#     # return list(instruments_collection.find({'deleted_at': None, "SM_EXPIRY_DATE": {'$gte': datetime.now(timezone.utc)}}, {"_id": 0}))
#     return list(instruments_collection.find({'deleted_at': None}, {"_id": 0}))


# @lru_cache(maxsize=10)
# def get_single_instrument_data(security_id):
#     return instruments_collection.find_one({"SECURITY_ID": int(security_id), 'deleted_at': None}, {"_id": 0})


# def get_instruments():
#     # resp = [(marketfeed.NSE_FNO, str(instrmt["SECURITY_ID"]), marketfeed.Ticker)
#     #         for instrmt in get_instruments_data()]
#     resp = [(marketfeed.NSE_FNO if instrmt['EXCH_ID'] == 'NSE' else marketfeed.BSE_FNO, str(instrmt["SECURITY_ID"]), marketfeed.Full)
#             for instrmt in get_instruments_data()]
#     print(resp)
#     return resp
#     # return [(marketfeed.NSE_FNO, "35089", marketfeed.Ticker), (marketfeed.NSE_FNO, "35089", marketfeed.Full)]


# def process_data(dhan_data):
#     # global prev_closing_prices_data
#     instmnt = get_single_instrument_data(dhan_data.get('security_id'))
#     lot_size = instmnt.get('LOT_SIZE')
#     # prev_close_price = float(prev_closing_prices_data.get(
#     #     dhan_data.get('security_id'), {}).get('prev_close', 0))
#     resp = {'name': instmnt['DISPLAY_NAME'],
#             'security_id': dhan_data.get('security_id')}
#     resp['cmp'] = float(dhan_data.get('LTP'))
#     resp['net_change'] = f"{resp['cmp'] - float(dhan_data.get('close')) :.2f}"
#     resp['percent_change'] = (resp['cmp'] - float(dhan_data.get('close'))) / \
#         float(dhan_data.get('close')) if float(
#             dhan_data.get('close')) else None
#     resp['vwap'] = f"{(resp['cmp'] - float(dhan_data.get('avg_price'))) :.2f}"
#     resp['oi'] = dhan_data.get('OI')
#     resp['oi_day_high'] = dhan_data.get('oi_day_high')
#     resp['oi_day_low'] = dhan_data.get('oi_day_low')
#     resp['open'] = dhan_data.get('open')
#     resp['close'] = dhan_data.get('close')
#     resp['high'] = dhan_data.get('high')
#     resp['low'] = dhan_data.get('low')
#     resp['net_demand_supply'] = f"{float(dhan_data.get('total_buy_quantity')) - float(dhan_data.get('total_sell_quantity')):.2f}"
#     resp['ratio'] = float(dhan_data.get('total_buy_quantity')) / \
#         float(dhan_data.get('total_sell_quantity')) if float(
#             dhan_data.get('total_sell_quantity')) else None
#     resp['lot'] = ((float(dhan_data.get('total_buy_quantity')) -
#                     float(dhan_data.get('total_sell_quantity'))) / lot_size) if float(dhan_data.get('total_sell_quantity')) else None

#     return resp


# def save_to_db(dhan_data):
#     collection.insert_one(dhan_data)
#     # logging.info("Data saved to MongoDB")
#     del dhan_data['_id']
#     return dhan_data


# async def emit_filtered_data(data):
#     await sio.emit('market_feed', data, room=str(data['security_id']))


# @ sio.event
# async def connect(sid, environ):
#     logging.info(f"Client connected: {sid}")


# @ sio.event
# async def subscribe(sid, feeds):
#     for feed in feeds:
#         await sio.enter_room(sid, feed)
#     logging.info(f"Client {sid} subscribed to feeds: {feeds}")


# async def fetch_market_data():
#     while True:
#         try:
#             inst = get_instruments()
#             data = marketfeed.DhanFeed(client_id, access_token, inst, version)
#             await data.connect()
#             while True:
#                 data.run_forever()
#                 response = data.get_data()
#                 print(response)
#                 # if response.get('type') == 'Previous Close':
#                 #     prev_closing_prices_data[response.get('security_id')] = {
#                 #         'prev_close': response.get('prev_close'),
#                 #         'prev_OI': response.get('prev_OI')}

#                 if response.get('type') == 'Full Data':
#                     save_to_db(response)
#                     await emit_filtered_data(process_data(response))
#                 await asyncio.sleep(1)
#                 # Periodic task every second
#         except (ConnectionResetError, websockets.exceptions.ConnectionClosedError):
#             logging.error(f"Error fetching market data: {e}")
#             traceback.print_exc()
#             try:
#                 await data.disconnect()
#             except Exception:
#                 traceback.print_exc()
#                 pass
#             await asyncio.sleep(5)

#         except asyncio.CancelledError:
#             logging.info("Task was cancelled, shutting down gracefully.")
#             await data.disconnect()
#             break

#         except Exception as e:
#             logging.error(f"Error fetching market data: {e}")
#             traceback.print_exc()
#             await asyncio.sleep(5)  # Wait before retrying
#             await data.disconnect()


# async def start_background_tasks(app):
#     app['fetch_market_data'] = asyncio.create_task(fetch_market_data())


# async def cleanup_background_tasks(app):
#     app['fetch_market_data'].cancel()
#     await app['fetch_market_data']


# app.on_startup.append(start_background_tasks)
# app.on_cleanup.append(cleanup_background_tasks)

# if __name__ == "__main__":
#     logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', handlers=[
#         logging.FileHandler("market_feed.log"),
#         logging.StreamHandler()
#     ])
#     # web.run_app(app, host='0.0.0.0', port=5000)
#     try:
#         loop = asyncio.get_event_loop()

#         # Create the web server and start it in the background
#         runner = web.AppRunner(app)
#         loop.run_until_complete(runner.setup())
#         site = web.TCPSite(runner, 'localhost', 5000)
#         loop.run_until_complete(site.start())

#         # Run the event loop
#         loop.run_forever()
#     except RuntimeError as e:
#         logging.error(f"Runtime error: {e}")
#     except Exception as e:
#         logging.error(f"Unhandled error: {e}")
#         traceback.print_exc()
#     finally:
#         loop.run_until_complete(runner.cleanup())
#         mongo_client.close()
        

        
from functools import lru_cache
import time
import traceback
from dhanhq import marketfeed
from datetime import datetime, timedelta, timezone
import asyncio
import nest_asyncio
import netifaces
import socketio
from aiohttp import web
import json
import websockets
from config import settings
from pymongo import MongoClient
import logging
import os
import ssl
import aiohttp
import asyncio

# Apply the nest_asyncio patch
nest_asyncio.apply()

# Read environment variables
client_id = settings.DHAN_CLIENT_ID
access_token = settings.DHAN_ACCESS_TOKEN

version = "v2"  # Mention Version and set to latest version 'v2'

# MongoDB setup
mongo_client = MongoClient(settings.MONGO_URI)
db = mongo_client[settings.MONGO_DATABASE]
collection = db[settings.MONGO_COLLECTION]
instruments_collection = db["ticker_info"]

# Create a Socket.IO server
sio = socketio.AsyncServer(async_mode='aiohttp',
                           ping_interval=25,  # Default is 25 seconds
                           ping_timeout=60,   # Default is 60 seconds
                           )
app = web.Application()
sio.attach(app)

async def index(request):
    return web.FileResponse('public/index.html')

# Setup routes for static files
app.router.add_get('/', index)
app.router.add_static('/', path='public', name='static')

@lru_cache(maxsize=128)
def get_instruments_data():
    return list(instruments_collection.find({'deleted_at': None}, {"_id": 0}))

@lru_cache(maxsize=10)
def get_single_instrument_data(security_id):
    return instruments_collection.find_one({"SECURITY_ID": int(security_id), 'deleted_at': None}, {"_id": 0})

def get_instruments():
    resp = [(marketfeed.NSE_FNO if instrmt['EXCH_ID'] == 'NSE' else marketfeed.BSE_FNO, str(instrmt["SECURITY_ID"]), marketfeed.Full)
            for instrmt in get_instruments_data()]
    return resp

def process_data(dhan_data):
    instmnt = get_single_instrument_data(dhan_data.get('security_id'))
    lot_size = instmnt.get('LOT_SIZE')
    resp = {'name': instmnt['DISPLAY_NAME'],
            'security_id': dhan_data.get('security_id')}
    resp['cmp'] = float(dhan_data.get('LTP'))
    resp['net_change'] = f"{resp['cmp'] - float(dhan_data.get('close')) :.2f}"
    resp['percent_change'] = (resp['cmp'] - float(dhan_data.get('close'))) / \
        float(dhan_data.get('close')) if float(
            dhan_data.get('close')) else None
    resp['vwap'] = f"{(resp['cmp'] - float(dhan_data.get('avg_price'))) :.2f}"
    resp['oi'] = dhan_data.get('OI')
    resp['oi_day_high'] = dhan_data.get('oi_day_high')
    resp['oi_day_low'] = dhan_data.get('oi_day_low')
    resp['open'] = dhan_data.get('open')
    resp['close'] = dhan_data.get('close')
    resp['high'] = dhan_data.get('high')
    resp['low'] = dhan_data.get('low')
    resp['net_demand_supply'] = f"{float(dhan_data.get('total_buy_quantity')) - float(dhan_data.get('total_sell_quantity')):.2f}"
    resp['ratio'] = float(dhan_data.get('total_buy_quantity')) / \
        float(dhan_data.get('total_sell_quantity')) if float(
            dhan_data.get('total_sell_quantity')) else None
    resp['lot'] = ((float(dhan_data.get('total_buy_quantity')) -
                    float(dhan_data.get('total_sell_quantity'))) / lot_size) if float(dhan_data.get('total_sell_quantity')) else None

    return resp

def save_to_db(dhan_data):
    collection.insert_one(dhan_data)
    del dhan_data['_id']
    return dhan_data

async def emit_filtered_data(data):
    await sio.emit('market_feed', data, room=str(data['security_id']))

@sio.event
async def connect(sid, environ):
    logging.info(f"Client connected: {sid}")

@sio.event
async def subscribe(sid, feeds):
    for feed in feeds:
        await sio.enter_room(sid, feed)
    logging.info(f"Client {sid} subscribed to feeds: {feeds}")

async def fetch_market_data():
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE

    # async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=ssl_context)) as session:
    #     while True:
    #         try:
    #             inst = get_instruments()
    #             data = marketfeed.DhanFeed(client_id, access_token, inst, version)
    #             await data.connect()
    #             while True:
    #                 data.run_forever()
    #                 response = data.get_data()
    #                 print(response)
    #                 if response.get('type') == 'Full Data':
    #                     save_to_db(response)
    #                     await emit_filtered_data(process_data(response))
    #                 await asyncio.sleep(1)  # Periodic task every second
    #         except websockets.exceptions.ConnectionClosedError as e:
    #             logging.error(f"WebSocket connection closed: {e}")
    #             traceback.print_exc()
    #             await asyncio.sleep(5)  # Wait before retrying
    #             continue

    #         except ssl.SSLError as e:
    #             logging.error(f"SSL error occurred: {e}")
    #             traceback.print_exc()
    #             await asyncio.sleep(1)  # Wait before retrying
    #             continue

    #         except asyncio.CancelledError:
    #             logging.info("Task was cancelled, shutting down gracefully.")
    #             await data.disconnect()
    #             break

    #         except Exception as e:
    #             logging.error(f"Error fetching market data: {e}")
    #             traceback.print_exc()
    #             await asyncio.sleep(5)  # Wait before retrying
    #             await data.disconnect()
    


async def main():
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False  # Disable hostname checking
    ssl_context.verify_mode = ssl.CERT_NONE  # Disable SSL verification

    connector = aiohttp.TCPConnector(ssl=ssl_context)

    while True:
        try:
            async with aiohttp.ClientSession(connector=connector) as session:
                inst = get_instruments()
                data = marketfeed.DhanFeed(client_id, access_token, inst, version)
                
                await data.connect()
                while True:
                    response = data.get_data()
                    if response:
                        print(response)
                        if response.get('type') == 'Full Data':
                            save_to_db(response)
                            await emit_filtered_data(process_data(response))
                    await asyncio.sleep(1)  # Periodic task every second
        except (websockets.exceptions.ConnectionClosedError, aiohttp.ClientConnectorError, ssl.SSLError) as e:
            logging.error(f"Connection issue: {e}")
            traceback.print_exc()
            await asyncio.sleep(5)  # Wait before retrying

# Run the main function
asyncio.run(main())


async def start_background_tasks(app):
    app['fetch_market_data'] = asyncio.create_task(fetch_market_data())

async def cleanup_background_tasks(app):
    app['fetch_market_data'].cancel()
    await app['fetch_market_data']

app.on_startup.append(start_background_tasks)
app.on_cleanup.append(cleanup_background_tasks)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', handlers=[
        logging.FileHandler("market_feed.log"),
        logging.StreamHandler()
    ])
    try:
        loop = asyncio.get_event_loop()

        # Create the web server and start it in the background
        runner = web.AppRunner(app)
        loop.run_until_complete(runner.setup())
        site = web.TCPSite(runner, 'localhost', 5000)
        loop.run_until_complete(site.start())

        # Run the event loop
        loop.run_forever()
    except RuntimeError as e:
        logging.error(f"Runtime error: {e}")
    except Exception as e:
        logging.error(f"Unhandled error: {e}")
        traceback.print_exc()
    finally:
        loop.run_until_complete(runner.cleanup())
        mongo_client.close()
