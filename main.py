

# live testing the best algorithm I have 

from config import * 
import ssl

volume_history = {}
first_price = defaultdict(int)  
creation_time = {}

watched_tokens = set()  
bought_coins = set()  
got_first_price = set() 
banned_coins = set() 

fake_balance = 100 
market_check = deque()

total_profit = [] 
real_balance = 100 
good_market = True
def get_volume(mint, seconds, trade_type=None):
    """Return volume over last X seconds for buy/sell or both."""
    if mint not in volume_history:
        return 0

    now = time.time()
    cutoff = now - seconds

    if trade_type is None:
        # total volume
        return (
            sum(v for t, v in volume_history[mint]["buy"] if t >= cutoff) +
            sum(v for t, v in volume_history[mint]["sell"] if t >= cutoff)
        )
    else:
        return sum(v for t, v in volume_history[mint][trade_type] if t >= cutoff)

async def subscribe():
    global good_market
    uri = f"wss://pumpportal.fun/api/data?api-key={pp_apikey}"
    sol_price = await get_sol_price() or 138
    print("Entering the trenches...") 

    ssl_ctx = ssl.create_default_context()
    ssl_ctx.check_hostname = False
    ssl_ctx.verify_mode = ssl.CERT_NONE

    while True: 
        try:
            async with websockets.connect(uri, ssl=ssl_ctx) as websocket:
                await websocket.send(json.dumps({"method": "subscribeNewToken"}))
                await websocket.send(json.dumps({"method": "subscribeMigration"}))

                async for message in websocket: 
                    try:
                        # if len(market_check) >= 10: 
                        #     if market_check[-10] + 5 > market_check[-1]: # we have less than 5 units profit over the last ten trades
                        #         if good_market: # if it was a good market
                        #             print("Bad Market, trading with fake balance")
                        #             good_market = False 
                        #     else: # profitted at least 5 units
                        #         if not good_market: # it was a bad market
                        #             print("Good Market, trading with real balance")
                        #             good_market = True 
                        
                        # while len(market_check) > 10: 
                        #     market_check.popleft()

                        data = json.loads(message)

                        mint = data.get("mint", "") 
                        if mint[-4:] != 'pump': 
                            banned_coins.add(mint)
                        if not mint or mint in bought_coins or mint in banned_coins: # buying coins only once 
                            continue
                        
                        ticker = mint[:3].upper()
                        tx_type = data.get("txType") 

                        sol_mc = data.get("marketCapSol", 0)
                        usd_mc = int(sol_mc * sol_price)
                        if not usd_mc: 
                            continue

                        if tx_type == "create": 
                            now = time.time()

                            if mint not in creation_time:
                                creation_time[mint] = now

                            if usd_mc > 1_000 and mint not in watched_tokens:
                                watched_tokens.add(mint) 

                                await websocket.send(json.dumps({
                                    "method": "subscribeTokenTrade",
                                    "keys": [mint]
                                }))

                        elif tx_type == "buy" or tx_type == "sell":     
                            link = f"https://pump.fun/coin/{mint}" 
                            # skip old coins
                            age = time.time() - creation_time[mint]
                            if age > 60:
                                banned_coins.add(mint) # skip coin forever 
                                continue 
                            if age < 3: 
                                continue 
                            sol_amount = data.get("solAmount") 
                            if not sol_amount:
                                continue 

                            now = time.time()
                            usd_value = sol_amount * sol_price
                            

                            if mint not in volume_history: # set up deque
                                volume_history[mint] = {"buy": deque(), "sell": deque()}

                            volume_history[mint][tx_type].append((now, usd_value)) # add volume 

                            cutoff = now - 60 # get rid of old prices
                            for trade_type in ("buy", "sell"):
                                deck = volume_history[mint][trade_type]
                                while deck and deck[0][0] < cutoff:
                                    deck.popleft()
                            
                            # dont want to buy coins at are only 5% above their open 
                            if mint not in got_first_price: 
                                first_price[mint] = usd_mc 
                                got_first_price.add(mint)  
                            if first_price[mint] * 1.5 > usd_mc: 
                                continue 

                            # skip coins with too much sell volume
                            if usd_mc: # filtering out coins based on volume   
                                buyVolume = get_volume(mint, 300, "buy") # only goes up to 60 btw
                                sellVolume = get_volume(mint, 300, "sell")
                                if not sellVolume: 
                                    sellVolume = 1
                                
                                if buyVolume < 1000: 
                                    continue 

                                volumeRatio = round((buyVolume / sellVolume), 2)   
                                if volumeRatio < 3: # want coins that are just going up  
                                    banned_coins.add(mint)   
                                    continue   
                                
                            if (12_000 < usd_mc < 20_000): # buy threshold. 
                                
                                bought_coins.add(mint)
                                print(f"Coin Number {len(bought_coins)} - {ticker}") 
                                print(f"Buying {link}")  
                                # print(f"First price: {first_price[mint]}") 
                                print(f"Buy Volume: {buyVolume}")           
                                print(f"Sell Volume: {sellVolume}")  
                                print(f"Volume Ratio: {volumeRatio}")  
                                print(f"Entering at mc {usd_mc} with 10 Units at time {time.strftime('%H:%M:%S', time.localtime())}")
                                print("\n")
                                entry = usd_mc 
                                await asyncio.sleep(0.5)  
                                # await handle_trade(mint, ticker, entry)
                                asyncio.create_task(handle_trade(mint, ticker, entry)) 


                    except Exception as msg_err:
                        print("Error processing message:", msg_err)

        except websockets.exceptions.ConnectionClosedError:
            print("⚠️ Connection dropped — reconnecting...")
            await asyncio.sleep(1)
            continue

        except Exception as e:
            print("Unexpected error:", e)
            await asyncio.sleep(1)
            continue
    
async def handle_trade(current_mint, ticker, entry): 
    global real_balance 
    global fake_balance
    global market_check
    global good_market
    uri = f"wss://pumpportal.fun/api/data?api-key={pp_apikey}"   
    ath = entry
    athTimer = None
    entry_units = 10
    sol_price = await get_sol_price()  
    if sol_price is None:
        sol_price = 138

    ssl_ctx = ssl.create_default_context()
    ssl_ctx.check_hostname = False
    ssl_ctx.verify_mode = ssl.CERT_NONE

    if good_market: # trade with real balance also update fake balance 
        async with websockets.connect(
            uri,
            ssl=ssl_ctx,
            ping_interval=None,
            ping_timeout=None,
            max_queue=None
        ) as websocket: 
            
            payload = {
                "method": "subscribeTokenTrade",
                "keys": [current_mint] 
            }
            await websocket.send(json.dumps(payload)) 

            start = time.time()
            while True: 
                start = time.time()
                try:
                    message = await asyncio.wait_for(websocket.recv(), timeout=1) 
                # since if theres no message the below if statement would never be reached
                except asyncio.TimeoutError:  
                    if time.time() - start > 5: # for new coins maybe even something like 5
                        if current_mc:  
                            #await asyncio.sleep(0.5) 
                            gain = ((current_mc - entry) / entry) * 100   
                            decimal = gain / 100
                            real_balance += (entry_units * decimal) 
                            fake_balance += (entry_units * decimal)
                            print(f"Timeout, selling all {ticker} at {current_mc} at time {time.strftime('%H:%M:%S', time.localtime())}, Margin: {gain:.2f}% ")    
                            break
                        
                        else:   
                            print(f"Timeout, selling all {ticker} at an unknown MC at time {time.strftime('%H:%M:%S', time.localtime())}")  
                            break  
                    continue  

                except Exception as e:
                    # we may not be certain of the price but we just have to sell all of the current coin 
                    # sell here 
                    #await asyncio.sleep(0.5)
                    print(f"Unexpected error: {e}") 
                    print(f"Selling all {ticker} at time {time.strftime('%H:%M:%S', time.localtime())}")  
                    break
                
                if athTimer and time.time() - athTimer > 15:
                    if current_mc:  
                        gain = ((current_mc - entry) / entry) * 100   
                        decimal = gain / 100
                        real_balance += (entry_units * decimal) 
                        fake_balance += (entry_units * decimal)
                        print(f"Coin went stagnant, selling all {ticker} at {current_mc} at time {time.strftime('%H:%M:%S', time.localtime())}, Margin: {gain:.2f}% ")   
                    
                    else:   
                        print(f"Coin went stagnant, selling all {ticker} at an unknown MC at time {time.strftime('%H:%M:%S', time.localtime())}")  
                    break
                    

                data = json.loads(message) 
            
                if not isinstance(data, dict):
                    continue
                
                tx_type = data.get("txType")
                sol_amount = data.get("solAmount")
                
                sol_mc = data.get("marketCapSol") 
                if sol_mc is None or not isinstance(sol_mc, (int, float)):
                    continue
                
                current_mc = int(sol_mc * sol_price) 
                mint = data.get("mint") 
                if mint is None:
                    continue
                
                if current_mc > 1_000:
                    if current_mc > ath: 
                        ath = current_mc  
                        athTimer = time.time() 

                    if current_mc < 20_000 and current_mc < (ath * 0.95) or current_mc < (entry * 0.95): 
                        gain = ((current_mc - entry) / entry) * 100 
                        decimal = gain / 100
                        real_balance += (entry_units * decimal) # sell whatevers left  
                        fake_balance += (entry_units * decimal)
                        print(f"""Stoploss reached
                            Selling all {ticker} at: {current_mc} 
                            Time: {time.strftime('%H:%M:%S', time.localtime())}, 
                            Margin: {gain:.2f}% """) 
                        break 

                    elif current_mc >= 20_000 and current_mc < (ath * 0.90) or current_mc < (entry * 0.95):   
                        gain = ((current_mc - entry) / entry) * 100 
                        decimal = gain / 100
                        real_balance += (entry_units * decimal) # sell whatevers left  
                        fake_balance += (entry_units * decimal)
                        print(f"""Stoploss reached high mc
                            Selling all {ticker} at: {current_mc} 
                            Time: {time.strftime('%H:%M:%S', time.localtime())}, 
                            Margin: {gain:.2f}% """) 
                        break   
                    
                # await asyncio.sleep(0) 

            try: 
                await websocket.send(json.dumps({
                        "method": "unsubscribeTokenTrade",
                        "keys": [current_mint] 
                    }))  
                # print("Unsubscribed!")
            except Exception as e: 
                print("Error:", e) 
            
            market_check.append((fake_balance)) 
            print(f"Real Balance: {round(real_balance, 2)}") 
            # print(market_check)
            return

    else: # if bad market still update fake balance
        #  print("TESTING MARKET CONDITIONS")
         async with websockets.connect(
            uri,
            ping_interval=None,
            ping_timeout=None,
            max_queue=None
        ) as websocket: 
            
            payload = {
                "method": "subscribeTokenTrade",
                "keys": [current_mint] 
            }
            await websocket.send(json.dumps(payload)) 

            start = time.time()
            while True: 
                start = time.time()
                try:
                    message = await asyncio.wait_for(websocket.recv(), timeout=1) 
                # since if theres no message the below if statement would never be reached
                except asyncio.TimeoutError:  
                    if time.time() - start > 5: # for new coins maybe even something like 5
                        if current_mc:  
                            #await asyncio.sleep(0.5) 
                            gain = ((current_mc - entry) / entry) * 100   
                            decimal = gain / 100
                            fake_balance += (entry_units * decimal)
                            print(f"Timeout, selling all {ticker} at {current_mc} at time {time.strftime('%H:%M:%S', time.localtime())}, Margin: {gain:.2f}% ")    
                            break
                        
                        else:   
                            print(f"Timeout, selling all {ticker} at an unknown MC at time {time.strftime('%H:%M:%S', time.localtime())}")  
                            break  
                    continue  

                except Exception as e:
                    # we may not be certain of the price but we just have to sell all of the current coin 
                    # sell here 
                    #await asyncio.sleep(0.5)
                    print(f"Unexpected error: {e}") 
                    print(f"Selling all {ticker} at time {time.strftime('%H:%M:%S', time.localtime())}")  
                    break
                
                if athTimer and time.time() - athTimer > 15:
                    if current_mc:  
                        gain = ((current_mc - entry) / entry) * 100   
                        decimal = gain / 100
                        fake_balance += (entry_units * decimal)
                        print(f"Coin went stagnant, selling all {ticker} at {current_mc} at time {time.strftime('%H:%M:%S', time.localtime())}, Margin: {gain:.2f}% ")   
                    
                    else:   
                        print(f"Coin went stagnant, selling all {ticker} at an unknown MC at time {time.strftime('%H:%M:%S', time.localtime())}")  
                    break
                    

                data = json.loads(message) 
            
                if not isinstance(data, dict):
                    continue
                
                tx_type = data.get("txType")
                sol_amount = data.get("solAmount")
                
                sol_mc = data.get("marketCapSol") 
                if sol_mc is None or not isinstance(sol_mc, (int, float)):
                    continue
                
                current_mc = int(sol_mc * sol_price) 
                mint = data.get("mint") 
                if mint is None:
                    continue
                
                if current_mc > 1_000:
                    if current_mc > ath: 
                        ath = current_mc  
                        athTimer = time.time() 

                    if current_mc < 20_000 and current_mc < (ath * 0.90) or current_mc < (entry * 0.95): 
                        gain = ((current_mc - entry) / entry) * 100 
                        decimal = gain / 100
                        fake_balance += (entry_units * decimal)
                        print(f"""Stoploss reached
                            Selling all {ticker} at: {current_mc} 
                            Time: {time.strftime('%H:%M:%S', time.localtime())}, 
                            Margin: {gain:.2f}% """) 
                        break 

                    elif current_mc >= 20_000 and current_mc < (ath * 0.85) or current_mc < (entry * 0.95):   
                        gain = ((current_mc - entry) / entry) * 100 
                        decimal = gain / 100
                        fake_balance += (entry_units * decimal)
                        print(f"""Stoploss reached high mc
                            Selling all {ticker} at: {current_mc} 
                            Time: {time.strftime('%H:%M:%S', time.localtime())}, 
                            Margin: {gain:.2f}% """) 
                        break   
                    
                # await asyncio.sleep(0) 

            try: 
                await websocket.send(json.dumps({
                        "method": "unsubscribeTokenTrade",
                        "keys": [current_mint] 
                    }))  
                # print("Unsubscribed!")
            except Exception as e: 
                print("Error:", e) 
            
            market_check.append((fake_balance))
            print(f"Fake Balance: {round(fake_balance, 2)}")
            # print(market_check)
            return

asyncio.run(subscribe())
