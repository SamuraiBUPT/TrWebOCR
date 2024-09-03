from fastapi import FastAPI, Request, Response
from contextlib import asynccontextmanager
import asyncio
import subprocess
import aiohttp
from collections import deque
from typing import Deque, Dict, Tuple

from loguru import logger

import uvicorn

ports = [8000 + i for i in range(1, 4)]

# global variables
index = 0
request_queue: Deque[Tuple[Request, asyncio.Future]] = deque()      # NOTE: use async lock!
in_flight_requests: Dict[int, int] = {}                             # NOTE: use async lock!
slot_status: Dict[int, int] = {}                                    # NOTE: use async lock!  为0的时候表示没事，为1的时候表示请勿再输送，正在重启
max_in_flight_requests = 5  # 5的时候是最高的，10反而会下降
lock = asyncio.Lock()

# 容灾操作
sub_processes = []
request_tracker: Dict[int, int] = {port : 0 for port in ports}      # NOTE: use async lock!
request_limits: Dict[int, int] = {}     # 每个子进程最大承载的请求数量，呈阶梯状分布，避免容灾失败。比如300， 400， 500

    
# # use lock operation
# async def get_request_future() -> tuple:
#     global lock
#     async with lock:
#         if request_queue:
#             request, future = request_queue.popleft()
#             return request, future
#         else:
#             return None, None


# 定义启动子进程的函数
async def start_subprocess(command: str):
    return await asyncio.create_subprocess_shell(
        command,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )
    
# Stop a subprocess
async def stop_subprocess(port: int):
    global sub_processes
    for process in sub_processes:
        if process.args.endswith(f"--port={port}"):
            process.terminate()
            await process.wait()
            break

# Restart a subprocess if it exceeds the request limit
async def restart_subprocess(port: int):
    command = f"python backend/main.py --open_gpu=1 --port={port}"
    # Stop the subprocess
    await stop_subprocess(port)
    # Start the subprocess again
    process = await start_subprocess(command)
    return process


@asynccontextmanager
async def lifespan(app: FastAPI):
    global ports, sub_processes, request_limits, in_flight_requests, slot_status
    # 子进程指令列表
    commands = [
        f"python backend/main.py --open_gpu=1 --port={port}" for port in ports
    ]
    
    # 启动子进程
    for command in commands:
        process = await start_subprocess(command)
        sub_processes.append(process)
        
    # 确保子进程启动成功
    for process in sub_processes:
        await asyncio.sleep(2)  # 可调整为合适的等待时间
        if process.returncode is not None:
            print(f"Subprocess failed to start: {process.returncode}")
            
    
    for port in ports:
        # 初始化in-flight记录器
        in_flight_requests[port] = 0
        
        # 初始化request_limits
        cnt = port - 8000
        request_limits[port] = 100 + (cnt-1) * 100
        
        # 初始化slot_status
        slot_status[port] = 0

    
    try:
        yield
    finally:
        # 停止所有子进程
        for process in sub_processes:
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
    
    try:
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
    except Exception as e:
        return b'{"code": 400, "msg": "starlette.requests.ClientDisconnect"}', 500, None
        
        
async def process_request_queue():
    global request_tracker, request_limits, lock
    while True:
        if request_queue:
            async with lock:
                request, future = request_queue.popleft()

            # 选择一个空闲的服务，赋值给selected_port
            selected_port = None
            async with lock:
                for port, count in in_flight_requests.items():
                    if count < max_in_flight_requests and slot_status[port] != 1:
                        selected_port = port
                        in_flight_requests[port] += 1
                        break

            if selected_port is not None:
                try:
                    # track the request
                    async with lock:
                        request_tracker[selected_port] += 1
                    
                    
                    # Check if the subprocess needs to be restarted
                    if request_tracker[selected_port] >= request_limits[selected_port]:
                        async with lock:
                            slot_status[selected_port] = 1  # 赋值为1，拒绝其他请求再进入
                            
                        
                        # 等待in-flight请求都运行完，才能继续往下重启
                        while True:
                            if in_flight_requests[selected_port] == 0:
                                break
                            
                        logger.debug("重启中...")
                        # Restart the subprocess
                        await restart_subprocess(selected_port)
                        await asyncio.sleep(5)
                        
                        logger.debug("重启成功!")
                        
                        async with lock:
                            request_tracker[selected_port] = 1  # Reset count after restart
                            slot_status[selected_port] = 0 # 允许其他请求进入
                            

                    content, status, headers = await forward_request_to_backend(request, selected_port)
                except aiohttp.client_exceptions.ClientOSError as e:
                    future.set_result(Response(content=b'{"code": 400, "message": "not valid!"}'), status_code=400)
                    return
                except aiohttp.client_exceptions.ServerDisconnectedError as e:
                    future.set_result(Response(content=b'{"code": 400, "message": "not valid!"}'), status_code=500)
                    return

                # 后处理逻辑：归位
                async with lock:
                    in_flight_requests[selected_port] -= 1
                    

                # 完成 Future，返回结果给请求者
                # print(f"Done: {content}")
                if headers is not None:
                    future.set_result(Response(content=content, status_code=status, headers=dict(headers)))
                else:
                    future.set_result(Response(content=content, status_code=status))
                return
            else:
                # 如果没有空闲服务，则请求重新入队
                async with lock:
                    request_queue.appendleft((request, future))
        
        await asyncio.sleep(0.1)  # 小的等待时间，避免无意义的空循环


@app.post("/api/tr-run")
async def tr_serve(request: Request):
    # 创建一个future对象
    future = asyncio.Future()
    
    # 将请求加入队列
    async with lock:
        request_queue.append((request, future))
        
    await process_request_queue()

    # 立即处理队列
    respnse = await future
    return await future


if __name__ == '__main__':
    uvicorn.run(app, host='0.0.0.0', port=6006)