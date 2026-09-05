
# testing a natural delay.  
# banning went_down coins and including volume ratio < 3
from config import *  
from datetime import datetime

volume_history = {}
first_price = defaultdict(int)  
creation_time = {} 
token_distribution = {} # mint -> {wallet: tokens held} 
top_ten_holders = {} # mint -> [sorted((wallet, amount))] 
ath_tracker = {} # mint -> ath. If we get a price a certain amount less than the ath we ban it 

watched_tokens = set()  
bought_coins = set()  
got_first_price = set() 
banned_coins = set()  
went_down = set()

total_profit = [] 
balance = 100  

SOL_PRICE = 139 

coins_sold = 0

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

def update_top_ten(mint, wallet, amount, top_ten_holders):
    # Ignore wallets with zero or negative balance
    if amount <= 0:
        # Remove wallet from list if it exists
        lst = [x for x in top_ten_holders[mint] if x[0] != wallet]
        top_ten_holders[mint] = lst
        return

    lst = top_ten_holders[mint]

    # Remove existing entry for this wallet
    lst = [x for x in lst if x[0] != wallet]

    # Case 1: fewer than 10 → always insert the positive balance
    if len(lst) < 10:
        lst.append((wallet, amount))
        lst.sort(key=lambda x: x[1], reverse=True)
        top_ten_holders[mint] = lst 
        return 

    # Case 2: full list → compare amount with current lowest
    lowest_wallet, lowest_amount = lst[-1]

    if amount > lowest_amount:
        lst.append((wallet, amount))
        lst.sort(key=lambda x: x[1], reverse=True)
        lst = lst[:10]  # keep only top 10

    top_ten_holders[mint] = lst

    # if len(lst) == 10: 
    #     percents = [round((amount / 1_000_000_000) * 100, 2)
    #                 for wallet, amount in top_ten_holders[mint]]
    #     print(f"Percents: {percents} Mint: {mint}") 
    return

def check_top_ten(mint):  
    lst = top_ten_holders[mint] 

    if len(lst) < 10:  
        # print("Less than ten holders")
        return "Less than ten holders", ""
    
    if len(lst) == 10:  
        percents = [round((amount / 1_000_000_000) * 100, 4)
                    for wallet, amount in lst] 
        if percents[0] > 5: 
            # print("Top Holder owns too much")  
            print(percents)
            return "Top Holder owns too much", percents  
        
        elif percents[0] < 1: 
            #print(percents) 
            return "Top Holder owns too little", percents 
        
        elif percents[0] + percents[1] + percents[2] > 10: 
            #print(percents) 
            return "Top 3 holders own too much", percents   
        
        elif percents[0] + percents[1] + percents[2] < 3: 
            #print(percents) 
            return "Top 3 holders own too little", percents  
        
        # elif all(1 <= percents[i] <= 2 for i in range(3)): 
        elif 1 <= percents[0] <= 2:
            #print(percents)
            return "Concentration great, double down", percents  
        
        else: 
            #print(percents)
            return "Concentration good", percents  
    
    # if somehow there is more than ten holders
    return "Unexpected holder count", lst 

async def execute(action, mint, amount):
    """
    action: 'buy' or 'sell'
    mint: mint address
    amount:
        - if buying: float SOL amount
        - if selling: int percent (0–100)
    """

    # ---- Build POST body ----
    

    if action == "buy": 
        data = {
        "publicKey": str(phantom_pubkey_test),
        "action": action,
        "mint": mint,
        "slippage": 50,
        "priorityFee": 0.000001,
        "pool": "auto",
                }
        data["amount"] = amount
        data["denominatedInSol"] = "true"

    else:  # sell 
        data = {
        "publicKey": str(phantom_pubkey_test),
        "action": action,
        "mint": mint,
        "slippage": 100,
        "priorityFee": 0.000001,
        "pool": "auto",
                }
        data["amount"] = f"{amount}%"
        data["denominatedInSol"] = "true"

    # ---- Make trade request ----
    async with aiohttp.ClientSession() as session:

        async with session.post(
            "https://pumpportal.fun/api/trade-local",
            data=data
        ) as resp:
            raw_bytes = await resp.read()

    # ---- Build transaction ----
    tx = VersionedTransaction(
        VersionedTransaction.from_bytes(raw_bytes).message,
        [phantom_privkey_test]
    )

    commitment = CommitmentLevel.Confirmed
    config = RpcSendTransactionConfig(preflight_commitment=commitment)
    send_payload = SendVersionedTransaction(tx, config).to_json()

    # ---- Send to RPC ----
    async with aiohttp.ClientSession() as session:
        async with session.post(
            "https://mainnet.helius-rpc.com/?api-key=9b4eae4a-bde0-42c0-8c04-4a505d5176d7",
            data=send_payload,
            headers={"Content-Type": "application/json"}
        ) as resp:
            rpc_data = await resp.json()

    # ---- Handle result ----
    if "result" in rpc_data:
        tx_sig = rpc_data["result"]
        print(f"{action.title()} Success")
        print(f"Transaction: https://solscan.io/tx/{tx_sig}") 

        # with open("results.txt", "a") as f:
        #     f.write(f"{action.title()} Success\n")
        #     f.write(f"Transaction: https://solscan.io/tx/{tx_sig}\n\n")
        
        return True, tx_sig

    else:
        # print("Transaction failed:", rpc_data)
        return None, rpc_data

async def main():
    uri = "wss://pumpportal.fun/api/data"
    sol_price = await get_sol_price() or SOL_PRICE
    print(f"Entering the trenches at {time.strftime('%H:%M:%S', time.localtime())}" + "\n")  
    

    while True: 
        try:
            async with websockets.connect(uri) as websocket:
                await websocket.send(json.dumps({"method": "subscribeNewToken"}))
                await websocket.send(json.dumps({"method": "subscribeMigration"}))

                async for message in websocket: 
                    try:
                        data = json.loads(message)

                        mint = data.get("mint", "") 
                        if not mint: 
                            continue 

                        if mint[-4:] != 'pump': 
                            banned_coins.add(mint)
                            # print(f"Banning coin {mint}")
                        if not mint or mint in bought_coins or mint in banned_coins: # buying coins only once 
                            continue
                        
                        ticker = mint[:3].upper()
                        tx_type = data.get("txType") 

                        sol_mc = data.get("marketCapSol", 0)
                        usd_mc = int(sol_mc * sol_price)
                        if not usd_mc or not tx_type: 
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
                            if mint not in creation_time:
                                continue 

                            link = f"https://pump.fun/coin/{mint}" 
                            

                            now = time.time()
                            sol_amount = data.get("solAmount") 
                            wallet = data.get("traderPublicKey")  
                            tokens = data.get("tokenAmount") # amount of tokens bought or sold
                            usd_value = sol_amount * sol_price  
                            if not sol_amount or not wallet or not tokens or not usd_value: 
                                continue   

                            age = time.time() - creation_time[mint]  
                            if age < 5: 
                                continue

                            # settin up ath tracker 
                            if mint not in ath_tracker: 
                                ath_tracker[mint] = usd_mc
                            
                            if usd_mc > ath_tracker[mint]: 
                                ath_tracker[mint] = usd_mc 
                            
                            if usd_mc < (ath_tracker[mint] * 0.75):  
                                # print(f"Banning {ticker}, price went too low below ath")  
                                # print(f"Link: {link}") 
                                # print("\n")
                                # banned_coins.add(mint) 
                                went_down.add(mint)

                            # setting up top ten holders 
                            if mint not in top_ten_holders: 
                                top_ten_holders[mint] = [] 

                            # setting up token distribution        
                            if mint not in token_distribution:
                                token_distribution[mint] = {}

                            if wallet not in token_distribution[mint]:
                                token_distribution[mint][wallet] = 0.0  

                            if tx_type == 'buy':
                                token_distribution[mint][wallet] += tokens

                            elif tx_type == 'sell':
                                token_distribution[mint][wallet] -= tokens

                            update_top_ten(
                                mint,
                                wallet,
                                token_distribution[mint][wallet],
                                top_ten_holders
                            )  
                            
                            # setting up volume 
                            if mint not in volume_history: 
                                volume_history[mint] = {"buy": deque(), "sell": deque()}

                            volume_history[mint][tx_type].append((now, usd_value)) # add volume 

                            cutoff = now - 60 # get rid of old prices
                            for trade_type in ("buy", "sell"):
                                deck = volume_history[mint][trade_type]
                                while deck and deck[0][0] < cutoff:
                                    deck.popleft()
                      
                            double_down = False
                            if 30_000 < usd_mc:  
                                # skip old coins
                                # age = time.time() - creation_time[mint]
                                # if age > 60:
                                #     banned_coins.add(mint)  
                                #     print(f"Age check failed: Coin older than 60 seconds. Time: {time.strftime('%H:%M:%S', time.localtime())}") 
                                #     print(f"Link: {link}")
                                #     continue 
                                

                                # checking volume  
                                buyVolume = get_volume(mint, 60, "buy") # only goes up to 60 btw
                                sellVolume = get_volume(mint, 60, "sell")
                                if not sellVolume: 
                                    sellVolume = 1
                                
                                if buyVolume < 500: 
                                    # print(f"Not enough volume. Time: {time.strftime('%H:%M:%S', time.localtime())}") 
                                    # print(f"Link: {link}") 
                                    # banned_coins.add(mint)
                                    continue 

                                # volumeRatio = round((buyVolume / sellVolume), 2)   
                                # if volumeRatio < 3: # want coins that are just going up  
                                #     banned_coins.add(mint)    
                                #     print(f"Volume check failed: Volume ratio was greater than 3. Time: {time.strftime('%H:%M:%S', time.localtime())}") 
                                #     print(f"Link: {link}") 
                                #     continue   

                                # checking distribution 
                                message, res = check_top_ten(mint) 
                                if message == "Less than ten holders":
                                    # print(f"{ticker} has less than ten holders {time.strftime('%H:%M:%S', time.localtime())}") 
                                    # print(f"Link: {link}")  
                                    continue 
                                
                                elif message == "Top Holder owns too much": 
                                    banned_coins.add(mint)
                                    print(res)
                                    print(f"Distribution check failed: Top Holder owns too much. Time: {time.strftime('%H:%M:%S', time.localtime())}") 
                                    print(f"Link: {link}") 
                                    print("\n") 
                                    continue   

                                elif message == "Top Holder owns too little": 
                                    banned_coins.add(mint) 
                                    print(f"Distribution check failed: Top Holder owns too little. Time: {time.strftime('%H:%M:%S', time.localtime())}")  
                                    print(res)
                                    print(f"Link: {link}") 
                                    print("\n") 
                                    continue  

                                elif message == "Top 3 holders own too much":  
                                    # banned_coins.add(mint)
                                    print(f"Distribution check would have failed: Top 3 Holders own too much. Time: {time.strftime('%H:%M:%S', time.localtime())}")  
                                    print(res)
                                    print(f"Link: {link}") 
                                    print("\n") 
                                    # continue  

                                # elif message == "Top 3 holders own too little":  
                                #     banned_coins.add(mint)
                                #     print(f"Distribution check failed: Top 3 Holders own too little. Time: {time.strftime('%H:%M:%S', time.localtime())}")  
                                #     print(res)
                                #     print(f"Link: {link}") 
                                #     print("\n") 
                                #     continue

                                # elif message == "Concentration great, double down": 
                                #     print(f"Concentration great for {ticker}, double down. Time: {time.strftime('%H:%M:%S', time.localtime())}")  
                                #     print(res)
                                #     # print(f"Link: {link}")  
                                #     double_down = True  

                                elif message == "Concentration good":  
                                # else:
                                    print("Concentration good")  
                                    print(res)


                                # if logic falls through, buy the coin
                                # if double_down: 
                                #     res, message = await execute("buy", mint, 0.002) 
                                # else: 
                                #     res, message = await execute("buy", mint, 0.001)   

                                # checking if it went down at any point 
                                if mint in went_down: 
                                    print(f"Would have bought {ticker}, but it went down. Time: {time.strftime('%H:%M:%S', time.localtime())}")    
                                    print(f"Link: {link}")  
                                    print("\n") 
                                    banned_coins.add(mint) 
                                    continue 

                                # res, message = await execute("buy", mint, 0.001) 
                                
                                # if not res: # seems to be always true
                                #     continue
 
                                
                                entry = usd_mc  
                                print(f"Initiating buy for {ticker} at {usd_mc} marketcap at time: {datetime.now().strftime('%H:%M:%S.%f')[:-4]}")  
                                print(f"Link: {link}")  
                                bought_coins.add(mint)
                                asyncio.create_task(delayed_buy(mint, ticker, entry, time.time()))  
                                
                                
                                
                                
                                # print(f"Buying {ticker} - {link}")  
                                # # print(f"First price: {first_price[mint]}") 
                                # print(f"Buy Volume: {buyVolume}")           
                                # print(f"Sell Volume: {sellVolume}")  
                                # print(f"Volume Ratio: {volumeRatio}") 
                                # print(f"{ath_tracker[mint]}") 
                                # print(f"Entering at mc {usd_mc} with 10 Units at time {time.strftime('%H:%M:%S', time.localtime())}")
                                # print("\n")
                                # entry = usd_mc 
                                # await asyncio.sleep(1)  
                                # # await handle_trade(mint, ticker, entry)
                                # asyncio.create_task(handle_trade(mint, ticker, entry))                              


                    except Exception as msg_err:
                        print("Error processing message:", msg_err)

        except websockets.exceptions.ConnectionClosedError:
            print("⚠️ Connection dropped — selling and reconnecting...") 
            await asyncio.sleep(1)
            continue

        except Exception as e:
            print("Unexpected error:", e)
            await asyncio.sleep(1)
            continue

async def delayed_buy(current_mint, ticker, entry, time_bought):  
    global balance 
    global coins_sold
    uri = "wss://pumpportal.fun/api/data"   
    ath = entry
    athTimer = None 
    sell_message = None
    mc_target = entry * 1.03 # what we need to hit every 5 seconds
    entry_units = 10
    sol_price = await get_sol_price()  
    if sol_price is None:
        sol_price = SOL_PRICE 
        
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
        current_mc = None
        while True: 
            try:
                message = await asyncio.wait_for(websocket.recv(), timeout=3)   

            except Exception as e: 
                print(f"Error: {e}")
            
            data = json.loads(message) 
        
            if not isinstance(data, dict):
                continue
            
            tx_type = data.get("txType")
            # sol_amount = data.get("solAmount") 
            mint = data.get("mint")  
            wallet = data.get("traderPublicKey")   
            tokens = data.get("tokenAmount") 
            sol_mc = data.get("marketCapSol")  

            if not tx_type or not mint or not wallet or not tokens or not sol_mc: 
                continue 
            
            # if sol_mc is None or not isinstance(sol_mc, (int, float)):
            #     continue
            
            current_mc = int(sol_mc * sol_price)  
            if not current_mc: 
                continue 
            link = f"https://pump.fun/coin/{mint}"  


            if time.time() - time_bought >= 0.35:  
                entry = current_mc 
                print(f"Actually buying {ticker} at {current_mc} marketcap at time: {datetime.now().strftime('%H:%M:%S.%f')[:-4]}")
                asyncio.create_task(handle_trade(mint, ticker, entry))       
                return   

async def delayed_sell(current_mint, ticker, entry, time_bought): 
    global balance 
    global coins_sold
    uri = "wss://pumpportal.fun/api/data"   
    ath = entry
    athTimer = None 
    sell_message = None
    mc_target = entry * 1.03 # what we need to hit every 5 seconds
    entry_units = 10
    sol_price = await get_sol_price()  
    if sol_price is None:
        sol_price = SOL_PRICE
    
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

        while True: 
            try:
                message = await asyncio.wait_for(websocket.recv(), timeout=3)   

            except Exception as e: 
                print(f"Error: {e}")  

            data = json.loads(message) 

            sol_mc = data.get("marketCapSol")  
            if not sol_mc: 
                 continue 
            current_mc = int(sol_mc * sol_price)  
            if not current_mc: 
                continue 

            if time.time() - time_bought >= 0.35:  
                print(f"Coin Number {coins_sold} - {ticker}") 
                print(f"Actually selling {ticker} at {current_mc} marketcap at time: {datetime.now().strftime('%H:%M:%S.%f')[:-4]}")
                return current_mc
            

async def handle_trade(current_mint, ticker, entry): 
    global balance 
    global coins_sold
    uri = "wss://pumpportal.fun/api/data"   
    ath = entry
    athTimer = None 
    sell_message = None
    mc_target = entry * 1.03 # what we need to hit every 5 seconds
    entry_units = 10
    sol_price = await get_sol_price()  
    if sol_price is None:
        sol_price = SOL_PRICE
    
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
        current_mc = None
        while True: 
            try:
                message = await asyncio.wait_for(websocket.recv(), timeout=3)  
                start = time.time() # valid message so reset stagnation timer
            # since if theres no message the below if statement would never be reached
            except asyncio.TimeoutError:  
                if time.time() - start > 5: # for new coins maybe even something like 5
                    if current_mc:  
                        # for attempt in range(11):
                        #     res, message = await execute("sell", current_mint, 100)
                        #     if res:  
                        print(f"Timeout")
                        print(f"Initiating sell for {ticker} at {current_mc} marketcap at time: {datetime.now().strftime('%H:%M:%S.%f')[:-4]}") 
                        mc = await delayed_sell(mint, ticker, current_mc, time.time()) 
                        gain = ((mc - entry) / entry) * 100 
                        decimal = gain / 100
                        balance += (entry_units * decimal)  
                        coins_sold += 1
                        print("\n")
                        break
                    
                            # Failed - retry if first attempt, otherwise give up
                            # if attempt < 10:
                            #     print(f"1 - A transaction error occurred, retrying attempt {attempt+1} at {time.strftime('%H:%M:%S', time.localtime())}")
                            #     await asyncio.sleep(1)
                            # else:
                            #     print(f"FAILURE - 1. Tried to sell {ticker}, unknown error at {time.strftime('%H:%M:%S', time.localtime())}", {message})
                            #     break
                        
                        # break
                  
                    else:    
                        # for attempt in range(11):
                            # res, message = await execute("sell", current_mint, 100)
                            # if res: 
                        coins_sold += 1 
                        print(f"Coin Number {coins_sold} - {ticker}") 
                        print(f"Timeout, selling all {ticker} at an unknown MC at time {time.strftime('%H:%M:%S', time.localtime())}")   
                        break 
                            
                            # Failed - retry if first attempt, otherwise give up
                            # if attempt < 10:
                            #     print(f"2 - A transaction error occurred, retrying attempt {attempt+1} at {time.strftime('%H:%M:%S', time.localtime())}")
                            #     await asyncio.sleep(1)
                            # else:
                            #     print(f"FAILURE - 2. Tried to sell {ticker}, unknown error at {time.strftime('%H:%M:%S', time.localtime())}", {message})
                            #     break
                        
                        # break
                continue  

            except Exception as e: 
                if current_mc:  
                    #await asyncio.sleep(0.5)
                    # for attempt in range(11):
                    #     res, message = await execute("sell", current_mint, 100)
                    #     if res:
                    mc, ticker = delayed_sell(mint, ticker, current_mc, time.time()) # type: ignore 
                    gain = ((mc - entry) / entry) * 100 
                    decimal = gain / 100
                    balance += (entry_units * decimal)  
                    coins_sold += 1 
                    # print(f"Coin Number {coins_sold} - {ticker}") 
                    # print(f"Unexpected error: {e}") 
                    # print(f"Selling all {ticker} at time {time.strftime('%H:%M:%S', time.localtime())}")  
                    break
                        
                        # Failed - retry if first attempt, otherwise give up
                        # if attempt < 10:
                        #     print(f"3 - A transaction error occurred, retrying attempt {attempt+1} at {time.strftime('%H:%M:%S', time.localtime())}")
                        #     await asyncio.sleep(1)
                        # else:
                        #     print(f"FAILURE - 3. Tried to sell {ticker}, unknown error at {time.strftime('%H:%M:%S', time.localtime())}", {message})
                        #     break
                else:    
                    # for attempt in range(11):
                    #     res, message = await execute("sell", current_mint, 100)
                    #     if res: 
                            coins_sold += 1 
                            print(f"Coin Number {coins_sold} - {ticker}")  
                            print(f"Timeout, selling all {ticker} at an unknown MC at time {time.strftime('%H:%M:%S', time.localtime())}")   
                            break
                        
                        # Failed - retry if first attempt, otherwise give up
                        # if attempt < 10:
                        #     print(f"4 - A transaction error occurred, retrying attempt {attempt+1} at {time.strftime('%H:%M:%S', time.localtime())}")
                        #     await asyncio.sleep(1)
                        # else:
                        #     print(f"FAILURE - 4. Tried to sell {ticker}, unknown error at {time.strftime('%H:%M:%S', time.localtime())}", {message})
                        #     break
                    
                    # break    
            
            if athTimer and time.time() - athTimer > 7:
                if current_mc:  
                    if current_mc > 20_000:
                        if time.time() - athTimer > 10:
                            # for attempt in range(11):
                            #     res, message = await execute("sell", current_mint, 100)
                            #     if res:
                                    print(f"High mc Coin went stagnant")  
                                    print(f"Initiating sell for {ticker} at {current_mc} marketcap at time: {datetime.now().strftime('%H:%M:%S.%f')[:-4]}") 
                                    mc = await delayed_sell(mint, ticker, current_mc, time.time()) 
                                    gain = ((mc - entry) / entry) * 100 
                                    decimal = gain / 100
                                    balance += (entry_units * decimal)  
                                    coins_sold += 1
                                    print("\n")
                                    break
                                
                                # if attempt < 10:
                                #     print(f"5 - A transaction error occurred, retrying attempt {attempt+1} at {time.strftime('%H:%M:%S', time.localtime())}")
                                #     await asyncio.sleep(1)
                                # else:
                                #     print(f"FAILURE - 5. Tried to sell {ticker}, unknown error at {time.strftime('%H:%M:%S', time.localtime())}", {message})
                                #     break
                            
                            # break

                        else: # wait longer for a high mc coin  
                            print(f"{ticker} would have timed out, but had high enough mc")
                            continue

                    #await asyncio.sleep(0.5)
                    if current_mc <= 20_000:
                        # for attempt in range(11):
                        #     res, message = await execute("sell", current_mint, 100)
                        #     if res:
                                print(f"Coin went stagnant")  
                                print(f"Initiating sell for {ticker} at {current_mc} marketcap at time: {datetime.now().strftime('%H:%M:%S.%f')[:-4]}") 
                                mc = await delayed_sell(mint, ticker, current_mc, time.time()) 
                                gain = ((mc - entry) / entry) * 100 
                                decimal = gain / 100
                                balance += (entry_units * decimal)  
                                coins_sold += 1
                                print("\n")
                                break
                        
                            # Failed - retry if first attempt, otherwise give up
                            # if attempt < 10:
                            #     print(f"5 - A transaction error occurred, retrying attempt {attempt+1} at {time.strftime('%H:%M:%S', time.localtime())}")
                            #     await asyncio.sleep(1)
                            # else:
                            #     print(f"FAILURE - 5. Tried to sell {ticker}, unknown error at {time.strftime('%H:%M:%S', time.localtime())}", {message})
                            #     break
                        
                        # break

                    
                else:  # unknown mc  
                # await asyncio.sleep(0.5) 
                    # for attempt in range(11):
                    #     res, message = await execute("sell", current_mint, 100)
                    #     if res: 
                            print(f"Coin Number {len(bought_coins)} - {ticker}") 
                            print(f"Coin went stagnant, selling all {ticker} at an unknown MC at time {time.strftime('%H:%M:%S', time.localtime())}")  
                            print("\n")
                            break
                        
                        # Failed - retry if first attempt, otherwise give up
                    #     if attempt < 10:
                    #         print(f"6 - A transaction error occurred, retrying attempt {attempt+1} at {time.strftime('%H:%M:%S', time.localtime())}")
                    #         await asyncio.sleep(1)
                    #     else:
                    #         print(f"FAILURE - 6. Tried to sell {ticker}, unknown error at {time.strftime('%H:%M:%S', time.localtime())}", {message})
                    #         break
                    
                    # break


            data = json.loads(message) 
        
            if not isinstance(data, dict):
                continue
            
            tx_type = data.get("txType")
            # sol_amount = data.get("solAmount") 
            mint = data.get("mint")  
            wallet = data.get("traderPublicKey")   
            tokens = data.get("tokenAmount") 
            sol_mc = data.get("marketCapSol")  

            if not tx_type or not mint or not wallet or not tokens or not sol_mc: 
                continue 
            
            # if sol_mc is None or not isinstance(sol_mc, (int, float)):
            #     continue
            
            current_mc = int(sol_mc * sol_price)  
            if not current_mc: 
                continue 
            link = f"https://pump.fun/coin/{mint}"  

            if wallet not in token_distribution[mint]:
                token_distribution[mint][wallet] = 0.0
        
            if tx_type == 'buy':
                token_distribution[mint][wallet] += tokens

            elif tx_type == 'sell':
                token_distribution[mint][wallet] -= tokens

            update_top_ten(
                mint,
                wallet,
                token_distribution[mint][wallet],
                top_ten_holders
            )

            top_ten_message, result = check_top_ten(mint) 
            
            if top_ten_message == "Top Holder owns too much":  
                # for attempt in range(11):
                #     res, message = await execute("sell", current_mint, 100)
                #     if res:
                        print(f"Initiating sell for {ticker} at {current_mc} marketcap at time: {datetime.now().strftime('%H:%M:%S.%f')[:-4]}") 
                        mc = delayed_sell(mint, ticker, current_mc, time.time()) 
                        gain = ((mc - entry) / entry) * 100 
                        decimal = gain / 100
                        balance += (entry_units * decimal)  
                        coins_sold += 1
                        # print(f"Coin Number {coins_sold} - {ticker}") 
                        # print(result)
                        # print(f"""Top Holder owns too much
                        #     Selling all {ticker} at: {current_mc} 
                        #     Time: {time.strftime('%H:%M:%S', time.localtime())}, 
                        #     Margin: {gain:.2f}%  
                        #     """)
                        break
                    
                    # Failed - retry if first attempt, otherwise give up
                #     if attempt < 10: 
                #         print(result)
                #         print(f"9 - A transaction error occurred, retrying attempt {attempt+1} at {time.strftime('%H:%M:%S', time.localtime())}")
                #         await asyncio.sleep(1)
                #     else:
                #         try: 
                #             print(result)
                #             print(f"9 - FAILURE. Tried to sell {ticker} at {time.strftime('%H:%M:%S', time.localtime())}, unknown error", {message})
                #             break
                #         except Exception as e:  
                #             print(result)
                #             print(f"9 - FAILURE. Tried to sell {ticker} at {time.strftime('%H:%M:%S', time.localtime())}, unknown error", {e})
                # break

            # elif top_ten_message == "Top Holder owns too little": 
            #     banned_coins.add(mint)
            #     print(f"Distribution check failed: Top Holder owns too little. Time: {time.strftime('%H:%M:%S', time.localtime())}") 
            #     print(f"Link: {link}") 
            #     print("\n") 
            #     continue  

            elif top_ten_message == "Top 3 holders own too much":  
                # for attempt in range(11):
                #     res, message = await execute("sell", current_mint, 100)
                #     if res:
                        print(f"Initiating sell for {ticker} at {current_mc} marketcap at time: {datetime.now().strftime('%H:%M:%S.%f')[:-4]}") 
                        mc = await delayed_sell(mint, ticker, current_mc, time.time()) 
                        gain = ((mc - entry) / entry) * 100 
                        decimal = gain / 100
                        balance += (entry_units * decimal)  
                        coins_sold += 1 
                        # print(f"Coin Number {coins_sold} - {ticker}")  
                        # print(result)
                        # print(f"""Top 3 Holders own too much
                        #     Selling all {ticker} at: {current_mc} 
                        #     Time: {time.strftime('%H:%M:%S', time.localtime())}, 
                        #     Margin: {gain:.2f}%  
                        #     """)
                        break
                    
                    # Failed - retry if first attempt, otherwise give up
                #     if attempt < 10: 
                #         print(result)
                #         print(f"10 - A transaction error occurred, retrying attempt {attempt+1} at {time.strftime('%H:%M:%S', time.localtime())}")
                #         await asyncio.sleep(1)
                #     else:
                #         try: 
                #             print(result)
                #             print(f"10 - FAILURE. Tried to sell {ticker} at {time.strftime('%H:%M:%S', time.localtime())}, unknown error", {message})
                #             break
                #         except Exception as e:  
                #             print(result)
                #             print(f"10 - FAILURE. Tried to sell {ticker} at {time.strftime('%H:%M:%S', time.localtime())}, unknown error", {e})
                # break

            # elif top_ten_message == "Top 3 holders own too little":  
            #     banned_coins.add(mint)
            #     print(f"Distribution check failed: Top 3 Holders own too little. Time: {time.strftime('%H:%M:%S', time.localtime())}") 
            #     print(f"Link: {link}") 
            #     print("\n") 
            #     continue

            # elif top_ten_message == "Concentration great, double down":  maybe can use this to buy more
            #     print(f"Concentration great, doubling down. Time: {time.strftime('%H:%M:%S', time.localtime())}") 
            #     print(f"Link: {link}") 
            #     print("\n")  
            #     double_down = True  

            if current_mc > 1_000:

                if current_mc > ath: 
                    ath = current_mc  
                    athTimer = time.time() 
                    mc_target = current_mc * 1.2 
                
                if current_mc < 20_000 and current_mc < (ath * 0.90) or current_mc < (entry * 0.95):    
                        print(f"Initiating sell for {ticker} at {current_mc} marketcap at time: {datetime.now().strftime('%H:%M:%S.%f')[:-4]}") 
                    # Retry logic: try twice, wait 0.5s between attempts
                    # for attempt in range(11):
                    #     res, message = await execute("sell", current_mint, 100)
                    #     if res: 
                        coins_sold += 1 
                        mc = await delayed_sell(mint, ticker, current_mc, time.time()) 
                        gain = ((mc - entry) / entry) * 100 
                        decimal = gain / 100
                        balance += (entry_units * decimal)  
                        # print(f"Coin Number {coins_sold} - {ticker}") 
                        # print(f"""Stoploss reached
                        #     Selling all {ticker} at: {current_mc} 
                        #     Time: {time.strftime('%H:%M:%S', time.localtime())}, 
                        #     Margin: {gain:.2f}%  
                        #     """)
                        break
                        
                        # Failed - retry if first attempt, otherwise give up
                    #     if attempt < 10:
                    #         print(f"7 - A transaction error occurred, retrying attempt {attempt+1} at {time.strftime('%H:%M:%S', time.localtime())}")
                    #         await asyncio.sleep(1)
                    #     else:
                    #         try:
                    #             print(f"7 - FAILURE. Tried to sell {ticker} at {time.strftime('%H:%M:%S', time.localtime())}, unknown error", {message})
                    #             break
                    #         except Exception as e: 
                    #             print(f"7 - FAILURE. Tried to sell {ticker} at {time.strftime('%H:%M:%S', time.localtime())}, unknown error", {e})
                    # break 

                elif current_mc >= 30_000 and current_mc < (ath * 0.8) or current_mc < (entry * 0.9): 
                    # for attempt in range(11):
                    #     res, message = await execute("sell", current_mint, 100)
                    #     if res:
                            print(f"Initiating sell for {ticker} at {current_mc} marketcap at time: {datetime.now().strftime('%H:%M:%S.%f')[:-4]}") 
                            mc = await delayed_sell(mint, ticker, current_mc, time.time()) 
                            gain = ((mc - entry) / entry) * 100 
                            decimal = gain / 100
                            balance += (entry_units * decimal)  
                            coins_sold += 1
                            # print(f"Coin Number {coins_sold} - {ticker}") 
                            # print(f"""Stoploss reached high mc
                            #     Selling all {ticker} at: {current_mc} 
                            #     Time: {time.strftime('%H:%M:%S', time.localtime())}, 
                            #     Margin: {gain:.2f}%  
                            #     """)
                            break
                        
                        # Failed - retry if first attempt, otherwise give up
                    #     if attempt < 10:
                    #         print(f"8 - A transaction error occurred, retrying attempt {attempt+1} at {time.strftime('%H:%M:%S', time.localtime())}")
                    #         await asyncio.sleep(1)
                    #     else:
                    #         try:
                    #             print(f"8 - FAILURE. Tried to sell {ticker}, unknown error at {time.strftime('%H:%M:%S', time.localtime())}", {message})
                    #             break
                    #         except Exception as e: 
                    #             print(f"8 - FAILURE. Tried to sell {ticker}, unknown error at {time.strftime('%H:%M:%S', time.localtime())}", {e})
                    # break 
                
            # await asyncio.sleep(0) 

        try: 
            await websocket.send(json.dumps({
                    "method": "unsubscribeTokenTrade",
                    "keys": [current_mint] 
                }))  
            # print("Unsubscribed!")
        except Exception as e: 
            print("Error:", e) 
        
        print(f"Balance: {round(balance, 2)}") 
        print("\n")
        return


asyncio.run(main())

# stagnation should be ath not just goes up, but goes up by a bit, dont want micro steps 
# maybe check for all of top ten
# test would have bought versus not 
# fix rejected https connection 