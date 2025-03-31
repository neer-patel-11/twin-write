import aiohttp
import asyncio

async def send_request(session, url, payload):
    async with session.post(url, json=payload) as response:
        print(f"Response ")

async def main():
    url = "http://localhost:8080/query"
    payload = {
        "query": "INSERT INTO users (name, email, password_hash) VALUES ('abc', 'abc@example.com', 'hashed_password212_here');"
    }

    async with aiohttp.ClientSession() as session:
        tasks = [send_request(session, url, payload) for _ in range(10000)]
        await asyncio.gather(*tasks)

asyncio.run(main())