import os
from dotenv import load_dotenv
import numbers
import asyncio
import aiohttp
from pydash import get

load_dotenv()

async def get_url(url):
    resp_data = False
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as resp:
                resp_data = await resp.json(content_type="application/json")
    except:
        print(f">>> ERROR getting url: {url} <<<")
    return resp_data

end_points = [
    {
        "name": "Tzkt public chain head",
        "key": "indexer_tzkt_public_chain_head",
        "env_address": "TZKT_ADDRESS_PUBLIC",
        "api_path":"/head",
        "level_path": "level"
    },
    {
        "name": "Tzkt local chain head",
        "key": "indexer_tzkt_local_chain_head",
        "env_address": "TZKT_ADDRESS",
        "api_path":"/head",
        "level_path": "level"
    }
]

async def get_head_levels():

    levels = []
    for p in end_points:
        rpc_url = os.getenv(p["env_address"])

        full_url = rpc_url + p["api_path"]
        print(full_url)
        head = await get_url(full_url)
        level = get(head, p["level_path"])
        if( isinstance(level, numbers.Number) ):
            levels.append(level)
        print(f"current level: {level}")

    return levels



async def get_head_levels_diff():
    levels = await get_head_levels()
    level_min = min(levels)
    level_max = max(levels)

    levels_diff = level_max - level_min
    return levels_diff

async def check_head_levels():
    print("checking head levels")
    levels_diff = await get_head_levels_diff()
    print("levels diff: ", levels_diff)
    msg_long = f"❌ *Tezos chain stats* script \nTZKT nodes out of sync with public RPC \nLevel difference: {levels_diff}"
    in_sync = levels_diff < 4

    msg = f"❌ nodes out if sync, level difference: {levels_diff}"

    if in_sync:
        msg = f"nodes in sync, level difference < 4: {levels_diff}"
        msg_long = f"nodes in sync, level difference < 4: {levels_diff}"
    return {
        "levels_diff": levels_diff,
        "in_sync": in_sync,
        "message": msg,
        "message_long": msg_long
    }


async def run_test():
    print("testing head levels")

    levels_sync_state = False
    try:
        levels_sync_state = await check_head_levels()
        print(levels_sync_state["message"])
    except Exception as e:
        print("error checking head levels:")
        print(e)
    

    if levels_sync_state == False:
        return False
    else:
        return {"passed":  levels_sync_state["in_sync"], "msg": levels_sync_state["message"]} 