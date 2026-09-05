
from config import *


async def get_bal(public_key):
    sol = await get_wallet_sol(public_key)
    if sol is None:
        print(f"Account: {public_key}")
        print("Balance: failed to fetch")
        return
    print(f"Account: {public_key}")
    print(f"Balance: {sol * 1_000_000_000:.0f} lamports")
    print(f"Balance: {sol} SOL")
    if sol < PP_MIN_SOL:
        print(f"WARNING: below PumpPortal minimum ({PP_MIN_SOL} SOL)")

if __name__ == "__main__":
    asyncio.run(get_bal(pp_pubkey))

# 249098744 lamports as of 7/22/2026 1:22 PM starting trades
# 72000 messages in 2 hours 