import asyncio
import aiohttp
from aiohttp import ClientSession, FormData

import time

# 定义一个异步函数来发送请求
async def fetch(session: ClientSession, url: str, file_path: str, sem: asyncio.Semaphore):
    async with sem:
        form = FormData()
        form.add_field('file', open(file_path, 'rb'), filename=file_path, content_type='application/octet-stream')
        # form.add_field('compress', '0')
        
        async with session.post(url, data=form) as response:
            if response.status == 200:
                results = await response.json()
                if 'data' in results and 'raw_out' in results['data']:
                    print(results['data']['raw_out'])
                return results
            else:
                print(f"Request failed with status {response.status}")
                return None

# 主函数，创建异步任务队列
async def main():
    url = 'http://localhost:6006/api/tr-run'
    sem = asyncio.Semaphore(20)  # 信号量，限制并发数量为20
    file_path = './attn.png'  # 假设有多个文件要处理

    async with aiohttp.ClientSession() as session:
        tasks = [fetch(session, url, file_path, sem) for _ in range(1000)]
        await asyncio.gather(*tasks)

# 启动异步任务
if __name__ == "__main__":
    start = time.time()
    asyncio.run(main())
    end = time.time()
    print(f"Time used: {end - start}s")