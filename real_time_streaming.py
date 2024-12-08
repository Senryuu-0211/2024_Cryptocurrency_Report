import websocket
import json
import pandas as pd
from datetime import datetime

data_list = []
row_count = 0
MAX_ROWS = 1000

def on_open(ws):
    sub_msg = {"method": "SUBSCRIBE", "params": ["btcusdt@kline_1m", "ethusdt@kline_1m", "bnbusdt@kline_1m", "xrpusdt@kline_1m", "solusdt@kline_1m"], "id": 1}
    ws.send(json.dumps(sub_msg))
    print("Opened connection")

def on_message(ws, message):
    global row_count, data_list
    data = json.loads(message)
    
    # Extract kline data
    kline = data['k'] if 'k' in data else data
    kline_data = {
        'symbol': kline['s'],
        'open_time': kline['t'],
        'open': float(kline['o']),
        'high': float(kline['h']), 
        'low': float(kline['l']),
        'close': float(kline['c']),
        'volume': float(kline['v']),
        'close_time': kline['T']
    }
    
    data_list.append(kline_data)
    row_count += 1
    print(f"Collected {row_count} rows")
    
    if row_count >= 100:
        save_to_csv()
        ws.close()

def save_to_csv():
    df = pd.DataFrame(data_list)
    filename = f"crypto_klines_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    df.to_csv(filename, index=False)
    print(f"Data saved to {filename}")

url = "wss://stream.binance.com:443/ws"

ws = websocket.WebSocketApp(url, on_open=on_open, on_message=on_message)
ws.run_forever()



