from fastapi import FastAPI, Request, Response
from contextlib import asynccontextmanager
import asyncio
import subprocess
import aiohttp
from collections import deque
from typing import Deque, Dict, Tuple

import uvicorn

ports = [8000 + i for i in range(1, 6)]

# global variables
index = 0
request_queue: Deque[Tuple[Request, asyncio.Future]] = deque()
in_flight_requests: Dict[int, int] = {}
max_in_flight_requests = 5  # 5的时候是最高的，10反而会下降
lock = asyncio.Lock()


# 定义启动子进程的函数
async def start_subprocess(command: str):
    return await asyncio.create_subprocess_shell(
        command,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )


@asynccontextmanager
async def lifespan(app: FastAPI):
    global ports
    # 子进程指令列表
    commands = [
        f"python backend/main.py --open_gpu=1 --port={port}" for port in ports
    ]
    
    # 启动子进程
    processes = []
    for command in commands:
        process = await start_subprocess(command)
        processes.append(process)
        
    # 确保子进程启动成功
    for process in processes:
        await asyncio.sleep(2)  # 可调整为合适的等待时间
        if process.returncode is not None:
            print(f"Subprocess failed to start: {process.returncode}")
            
    
    for port in ports:
        in_flight_requests[port] = 0

    
    try:
        yield
    finally:
        # 停止所有子进程
        for process in processes:
            process.terminate()
            await process.wait()
            

app = FastAPI(lifespan=lifespan)


# 转发请求到后端服务的函数
async def forward_request_to_backend(request: Request, selected_port: int):
    global index
    url = f"http://localhost:{selected_port}/api/tr-run/"
    
    # # 更新索引以实现轮询
    # index = (index + 1) % len(ports)
    
    headers = {key: value for key, value in request.headers.items() if key != "host"}
    
    async with aiohttp.ClientSession() as session:
        async with session.request(
            method=request.method,
            url=url,
            headers=headers,
            data=await request.body(),
            cookies=request.cookies
        ) as response:
            content = await response.read()
            return content, response.status, response.headers
        
        
async def process_request_queue():
    while True:
        if request_queue:
            async with lock:
                request, future = request_queue.popleft()

            # 选择一个空闲的服务
            selected_port = None
            async with lock:
                for port, count in in_flight_requests.items():
                    if count < max_in_flight_requests:
                        selected_port = port
                        in_flight_requests[port] += 1
                        break

            if selected_port is not None:
                content, status, headers = await forward_request_to_backend(request, selected_port)
                async with lock:
                    in_flight_requests[selected_port] -= 1

                # 完成 Future，返回结果给请求者
                # print(f"Done: {content}")
                future.set_result(Response(content=content, status_code=status, headers=dict(headers)))
                return
            else:
                # 如果没有空闲服务，则请求重新入队
                async with lock:
                    request_queue.appendleft((request, future))
        
        await asyncio.sleep(0.1)  # 小的等待时间，避免无意义的空循环


@app.post("/api/tr_run")
async def tr_serve(request: Request):
    # 创建一个future对象
    future = asyncio.Future()
    
    # 将请求加入队列
    async with lock:
        request_queue.append((request, future))
        
    await process_request_queue()

    # 立即处理队列
    return await future


if __name__ == '__main__':
    uvicorn.run(app, host='0.0.0.0', port=6006)