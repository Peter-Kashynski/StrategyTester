# buy at 8k mc, same sell logic as includingGT60
from config import * 
import ssl

creation_time = {} 
token_distribution = {} # mint -> {wallet: tokens held} 
top_ten_holders = {} # mint -> [sorted((wallet, amount))] 

watched_tokens = set()  
bought_coins = set()  
banned_coins = set()  

balance = 100  
SOL_PRICE = 136 
coins_sold = 0


def update_top_ten(mint, wallet, amount, top_ten_holders):
    if amount <= 0:
        lst = [x for x in top_ten_holders[mint] if x[0] != wallet]
        top_ten_holders[mint] = lst
        return

    lst = top_ten_holders[mint]
    lst = [x for x in lst if x[0] != wallet]

    if len(lst) < 10:
        lst.append((wallet, amount))
        lst.sort(key=lambda x: x[1], reverse=True)
        top_ten_holders[mint] = lst 
        return 

    lowest_wallet, lowest_amount = lst[-1]

    if amount > lowest_amount:
        lst.append((wallet, amount))
        lst.sort(key=lambda x: x[1], reverse=True)
        lst = lst[:10]

    top_ten_holders[mint] = lst
    return


def check_top_ten(mint):  
    lst = top_ten_holders[mint] 

    if len(lst) < 10:  
        return "Less than ten holders", ""
    
    if len(lst) == 10:  
        percents = [round((amount / 1_000_000_000) * 100, 4)
                    for wallet, amount in lst] 
        if percents[0] > 5: 
            print(percents)
            return "Top Holder owns too much", percents  
        
        elif percents[0] < 1: 
            return "Top Holder owns too little", percents 
        
        elif percents[0] + percents[1] + percents[2] > 10: 
            return "Top 3 holders own too much", percents   
        
        elif percents[0] + percents[1] + percents[2] < 3: 
            return "Top 3 holders own too little", percents  
        
        elif 1 <= percents[0] <= 2:
            return "Concentration great, double down", percents  
        
        else: 
            return "Concentration good", percents  
    
    return "Unexpected holder count", lst 


async def main():
    uri = f"wss://pumpportal.fun/api/data?api-key={pp_apikey}"
    sol_price = await get_sol_price() or SOL_PRICE
    print(f"Entering the trenches at {time.strftime('%H:%M:%S', time.localtime())}" + "\n")  

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
                        data = json.loads(message)

                        mint = data.get("mint", "") 
                        if not mint: 
                            continue 

                        if mint[-4:] != 'pump': 
                            banned_coins.add(mint)
                        if mint in bought_coins or mint in banned_coins:
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
                            sol_amount = data.get("solAmount") 
                            wallet = data.get("traderPublicKey")  
                            tokens = data.get("tokenAmount")
                            if not sol_amount or not wallet or not tokens: 
                                continue   

                            # track holders for sell-side distribution checks
                            if mint not in top_ten_holders: 
                                top_ten_holders[mint] = [] 

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
                            
                            # buy when mc hits 8k
                            if usd_mc >= 8_000:
                                bought_coins.add(mint)
                                print(f"Buying {ticker} - {link}")  
                                print(f"Entering at mc {usd_mc} with 10 Units at time {time.strftime('%H:%M:%S', time.localtime())}")
                                print("\n")
                                entry = usd_mc 
                                await asyncio.sleep(1)  
                                asyncio.create_task(handle_trade(mint, ticker, entry))                                    


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
    
async def handle_trade(current_mint, ticker, entry): 
    global balance 
    global coins_sold
    uri = f"wss://pumpportal.fun/api/data?api-key={pp_apikey}"   
    ath = entry
    athTimer = None 
    entry_units = 10
    sol_price = await get_sol_price()  
    if sol_price is None:
        sol_price = SOL_PRICE

    ssl_ctx = ssl.create_default_context()
    ssl_ctx.check_hostname = False
    ssl_ctx.verify_mode = ssl.CERT_NONE
    
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
        current_mc = None
        while True: 
            try:
                message = await asyncio.wait_for(websocket.recv(), timeout=3)  
                start = time.time()
            except asyncio.TimeoutError:  
                if time.time() - start > 5:
                    if current_mc:  
                        gain = ((current_mc - entry) / entry) * 100 
                        decimal = gain / 100
                        balance += (entry_units * decimal)  
                        coins_sold += 1 
                        print(f"Coin Number {coins_sold} - {ticker}") 
                        print(f"Timeout, selling all {ticker} at {current_mc} at time {time.strftime('%H:%M:%S', time.localtime())}, Margin: {gain:.2f}% ") 
                        break
                    
                    else:    
                        coins_sold += 1 
                        print(f"Coin Number {coins_sold} - {ticker}") 
                        print(f"Timeout, selling all {ticker} at an unknown MC at time {time.strftime('%H:%M:%S', time.localtime())}")   
                        break
                continue  

            except Exception as e: 
                if current_mc:  
                    gain = ((current_mc - entry) / entry) * 100 
                    decimal = gain / 100
                    balance += (entry_units * decimal)  
                    coins_sold += 1 
                    print(f"Coin Number {coins_sold} - {ticker}") 
                    print(f"Unexpected error: {e}") 
                    print(f"Selling all {ticker} at time {time.strftime('%H:%M:%S', time.localtime())}")  
                    break
                else:    
                    coins_sold += 1 
                    print(f"Coin Number {coins_sold} - {ticker}")  
                    print(f"Timeout, selling all {ticker} at an unknown MC at time {time.strftime('%H:%M:%S', time.localtime())}")   
                    break
            
            if athTimer and time.time() - athTimer > 7:
                if current_mc:  
                    if current_mc > 20_000:
                        if time.time() - athTimer > 10:
                            gain = ((current_mc - entry) / entry) * 100 
                            decimal = gain / 100
                            balance += (entry_units * decimal) 
                            coins_sold += 1 
                            print(f"Coin Number {coins_sold} - {ticker}") 
                            print(f"High mc Coin went stagnant, selling all {ticker} at {current_mc} at time {time.strftime('%H:%M:%S', time.localtime())}, Margin: {gain:.2f}% ")  
                            print("\n")
                            break

                        else:  
                            print(f"{ticker} would have timed out, but had high enough mc")
                            continue

                    if current_mc <= 20_000:
                        gain = ((current_mc - entry) / entry) * 100 
                        decimal = gain / 100
                        balance += (entry_units * decimal) 
                        coins_sold += 1 
                        print(f"Coin Number {coins_sold} - {ticker}") 
                        print(f"Coin went stagnant, selling all {ticker} at {current_mc} at time {time.strftime('%H:%M:%S', time.localtime())}, Margin: {gain:.2f}% ")  
                        print("\n")
                        break

                    
                else:  
                    print(f"Coin Number {len(bought_coins)} - {ticker}") 
                    print(f"Coin went stagnant, selling all {ticker} at an unknown MC at time {time.strftime('%H:%M:%S', time.localtime())}")  
                    print("\n")
                    break


            data = json.loads(message) 
        
            if not isinstance(data, dict):
                continue
            
            tx_type = data.get("txType")
            mint = data.get("mint")  
            wallet = data.get("traderPublicKey")   
            tokens = data.get("tokenAmount") 
            sol_mc = data.get("marketCapSol")  

            if not tx_type or not mint or not wallet or not tokens or not sol_mc: 
                continue 
            
            current_mc = int(sol_mc * sol_price)  
            if not current_mc: 
                continue 

            if mint not in token_distribution:
                token_distribution[mint] = {}
            if mint not in top_ten_holders:
                top_ten_holders[mint] = []

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
                gain = ((current_mc - entry) / entry) * 100 
                decimal = gain / 100
                balance += (entry_units * decimal)  
                coins_sold += 1 
                print(f"Coin Number {coins_sold} - {ticker}") 
                print(result)
                print(f"""Top Holder owns too much
                    Selling all {ticker} at: {current_mc} 
                    Time: {time.strftime('%H:%M:%S', time.localtime())}, 
                    Margin: {gain:.2f}%  
                    """)
                break

            elif top_ten_message == "Top 3 holders own too much":  
                gain = ((current_mc - entry) / entry) * 100 
                decimal = gain / 100
                balance += (entry_units * decimal)  
                coins_sold += 1 
                print(f"Coin Number {coins_sold} - {ticker}")  
                print(result)
                print(f"""Top 3 Holders own too much
                    Selling all {ticker} at: {current_mc} 
                    Time: {time.strftime('%H:%M:%S', time.localtime())}, 
                    Margin: {gain:.2f}%  
                    """)
                break

            if current_mc > 1_000:

                if current_mc > ath: 
                    ath = current_mc  
                    athTimer = time.time() 
                
                if current_mc < 20_000 and current_mc < (ath * 0.90) or current_mc < (entry * 0.95):   
                    gain = ((current_mc - entry) / entry) * 100 
                    decimal = gain / 100
                    balance += (entry_units * decimal)  
                    coins_sold += 1 
                    print(f"Coin Number {coins_sold} - {ticker}") 
                    print(f"""Stoploss reached
                        Selling all {ticker} at: {current_mc} 
                        Time: {time.strftime('%H:%M:%S', time.localtime())}, 
                        Margin: {gain:.2f}%  
                        """)
                    break

                elif current_mc >= 20_000 and current_mc < (ath * 0.85) or current_mc < (entry * 0.95): 
                    gain = ((current_mc - entry) / entry) * 100 
                    decimal = gain / 100
                    balance += (entry_units * decimal) 
                    coins_sold += 1 
                    print(f"Coin Number {coins_sold} - {ticker}") 
                    print(f"""Stoploss reached high mc
                        Selling all {ticker} at: {current_mc} 
                        Time: {time.strftime('%H:%M:%S', time.localtime())}, 
                        Margin: {gain:.2f}%  
                        """)
                    break

        try: 
            await websocket.send(json.dumps({
                    "method": "unsubscribeTokenTrade",
                    "keys": [current_mint] 
                }))  
        except Exception as e: 
            print("Error:", e) 
        
        print(f"Balance: {round(balance, 2)}") 
        print("\n")
        return


asyncio.run(main())
