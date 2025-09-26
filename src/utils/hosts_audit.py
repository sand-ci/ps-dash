import socket, ssl
import asyncio
from contextlib import suppress
import aiohttp
import pandas as pd
import urllib3
import model.queries as qrs

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

ES_INDICES = ["ps_trace", "ps_throughput", "ps_owd"]
LOOKBACK_DAYS = 30
VERIFY_TLS = False

PORT = 443
CONNECT_TIMEOUT = 3
HTTP_TIMEOUT = aiohttp.ClientTimeout(total=5)
RETRIES = 3
BACKOFF = [0, 5, 15]
STATUSES = ["ACTIVE_HTTP", "ACTIVE_TCP_ONLY", "UNREACHABLE_CANDIDATE", "RETIRED_DNS"]

async def resolve(host):
    with suppress(Exception):
        return await asyncio.get_running_loop().getaddrinfo(
            host, PORT, type=socket.SOCK_STREAM
        )
    return []

async def tcp_connect(addr):
    try:
        fut = asyncio.open_connection(addr[4][0], addr[4][1], ssl=False)
        r, w = await asyncio.wait_for(fut, timeout=CONNECT_TIMEOUT)
        w.close(); await w.wait_closed()
        return True
    except Exception:
        return False

async def http_probe(host):
    try:
        ctx = ssl.create_default_context()
        async with aiohttp.ClientSession(timeout=HTTP_TIMEOUT) as s:
            async with s.head(f"https://{host}/pscheduler", ssl=ctx) as resp:
                return 200 <= resp.status < 500
    except Exception:
        return False



async def audit_host(host):
    print(f"Auditing {host}...")
    result = {"host": host}
    result["found_in_ES"], result["netsite"], result["rcsite"] = qrs.hostFoundInES(host, LOOKBACK_DAYS, ES_INDICES)
    addrs = await resolve(host)
    if not addrs:
        result["status"] = "RETIRED_DNS"
        return result

    # Retry with backoff
    try:
        tcp_ok = http_ok = False
        for i in range(RETRIES):
            if i: await asyncio.sleep(BACKOFF[i])
            # try each resolved address until one works
            tcp_ok = any([await tcp_connect(a) for a in addrs])
            http_ok = await http_probe(host)
            if tcp_ok or http_ok:
                break
        if http_ok:
            result["status"] = "ACTIVE_HTTP"
        elif tcp_ok:
            result["status"] = "ACTIVE_TCP_ONLY"
        else:
            result["status"] = "UNREACHABLE_CANDIDATE"
            
    except Exception as e:
        print(e)
        result["status"] = "ERROR"
        return result
        
    return result

async def hosts_audit(hosts, concurrency=32):
    sem = asyncio.Semaphore(concurrency)
    async def wrapped(h):
        async with sem:
            return await audit_host(h)
    return await asyncio.gather(*[wrapped(h) for h in hosts])

async def audit(hosts_1):
    # run the audit and return results
    audited_hosts = await hosts_audit(hosts_1)
    return audited_hosts

