


from config import *


async def checkBalance(address):
    rpc = AsyncClient("https://api.mainnet-beta.solana.com")

    async with rpc:
        # Get account balance
        balance = await rpc.get_balance(address)

        # print(f"Account: {address}")
        # print(f"Balance: {balance.value} lamports")
        print(f"Balance: {balance.value / 1_000_000_000} SOL")

async def send_sol(sender, recipient, sol_amount): # priv_key, pub_key, amount in sol 
    shortened_sender = str(sender)[:3] + "..." + str(sender)[-3:] 
    shortened_recipient = str(recipient)[:3] + "..." + str(recipient)[-3:]
    print(f"Sending {sol_amount} Sol from {shortened_sender} to {shortened_recipient}")
    rpc = AsyncClient("https://api.mainnet-beta.solana.com")

    LAMPORTS_PER_SOL = 1_000_000_000
    transfer_amount = int(sol_amount * LAMPORTS_PER_SOL)

    async with rpc:
        # Get latest blockhash
        latest_blockhash = await rpc.get_latest_blockhash()

        # Create transfer instruction
        transfer_instruction = transfer(
            TransferParams(
                from_pubkey=sender.pubkey(),
                to_pubkey=recipient,
                lamports=transfer_amount
            )
        )

        # Create message
        message = MessageV0.try_compile(
            payer=sender.pubkey(),
            instructions=[transfer_instruction],
            address_lookup_table_accounts=[],
            recent_blockhash=latest_blockhash.value.blockhash
        )

        # Create transaction
        transaction = VersionedTransaction(message, [sender])
        signature = await rpc.send_transaction(transaction)  

        print("Success!") 
        print(f"View on Solscan: https://solscan.io/tx/{signature.value}")


async def send_all_sol(sender, recipient):
    """Drain sender: send balance − fee so the account ends at ~0 SOL."""
    TRANSFER_FEE_LAMPORTS = 5_000
    LAMPORTS_PER_SOL = 1_000_000_000

    rpc = AsyncClient("https://api.mainnet-beta.solana.com")
    async with rpc:
        bal = await rpc.get_balance(sender.pubkey())
        balance = int(bal.value)

    if balance <= TRANSFER_FEE_LAMPORTS:
        raise RuntimeError(
            f"Balance too low to drain ({balance} lamports; need > {TRANSFER_FEE_LAMPORTS} for fee)"
        )

    transfer_amount = balance - TRANSFER_FEE_LAMPORTS
    await send_sol(sender, recipient, transfer_amount / LAMPORTS_PER_SOL)


if __name__ == "__main__":
    asyncio.run(send_all_sol(pp_privkey, account_pubkey)) # priv_key, pub_key, amount in sol 
    # asyncio.run(send_all_sol(pp_privkey, account_pubkey))
