import time
import numpy as np
from tr import tr
from PIL import Image, ImageDraw
import datetime
import json
from PIL import Image
from io import BytesIO

from loguru import logger

from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.responses import JSONResponse
import uvicorn

class NpEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        else:
            return super(NpEncoder, self).default(obj)

def inference(img: Image):
    '''

    :return:
    报错：
    400 没有请求参数

    '''
    start_time = time.time()

    # 获取图像的宽度和高度
    img_width, img_height = img.size
    # 打印图像的尺寸
    if img_width < img_height:
        img = img.rotate(90, expand=True)
        
    img = img.convert("RGB")
    original_img = img

    # 进行ocr
    direction_is_right = False

    for rotation in [0, 180, 270, 90]:
        if rotation != 0:
            img = original_img.copy().rotate(rotation, expand=True)
            
        # main inference entrance
        res = tr.run(img.copy().convert("L"), flag=tr.FLAG_ROTATED_RECT)
        
        plain_text = '|'.join([item[1] for item in res])
        if '年' in plain_text or '登记' in plain_text or '统一' in plain_text or '营' in plain_text:
            direction_is_right = True
            break
            
    if '年' not in plain_text and '登记' not in plain_text and '统一' not in plain_text and '营' not in plain_text:
        plain_text += '-----------问题数据-----------'
        
        
    
    
    response_data = {'code': 200, 'msg': '成功',
                        'data': {'raw_out': plain_text + '------' + str(rotation),
                                # 'image_size': ['1','2'],
                                'speed_time': round(time.time() - start_time, 2)}}
    log_info = {
        # 'ip': self.request.host,
        # 'return': response_data,
        'time': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'latency': time.time() - start_time
        # 'result': plain_text
    }
    
    logger.info(json.dumps(log_info, cls=NpEncoder))
    return response_data

app = FastAPI()

@app.post("/api/tr-run")
async def tr_serve(file: UploadFile = File(...)):
    try:
        # # 检查文件类型是否为图像类型
        # if not file.content_type.startswith("image/"):
        #     return JSONResponse(status_code=400, content={"message": "上传的文件不是图片"})

        # 读取图片并转化为PIL.Image
        image_data = await file.read()
        img = Image.open(BytesIO(image_data))

        # 调用inference函数处理图片
        response_data = inference(img)
        
        return JSONResponse(content=response_data)

    except Exception as e:
        return JSONResponse(status_code=500, content={"message": str(e)})


if __name__ == '__main__':
    # 创建ArgumentParser对象
    import argparse
    parser = argparse.ArgumentParser(description="Run FastAPI server")

    # 添加port参数，默认为6006
    parser.add_argument('--port', type=int, default=6006, help='Port to run the FastAPI server on')

    # 解析命令行参数
    args = parser.parse_args()

    # 使用指定的端口运行服务器
    uvicorn.run(app, host='0.0.0.0', port=args.port)