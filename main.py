import json
from urllib.request import urlopen
from flask import Flask, jsonify
import time
from datetime import datetime
import schedule
import pytz
from urllib.parse import quote_plus, unquote, urlencode
import aiohttp
import redis 
import asyncio
from multiprocessing import Process

# redis 연결 
redis = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True,password=['password'])

# 코스피 코스닥 구분 전역변수
cheak = True

# redis 연결 확인   
print("redis연결 상태" + str(redis.ping()))

# 전역변수 선언확인
print("시장 구분" + str(cheak))

def job_wrapper():
    # 서울 시간 호출 
    seoul_time = pytz.timezone('Asia/Seoul')
    current_time = datetime.now(seoul_time)

    # 현재 시간이 오전 9시부터 오후 3시 사이인지 확인 (체크 할것), 주말은 제외
    if current_time.hour >= 9 and current_time.hour < 15 and current_time.weekday() < 5:
        asyncio.run(stock_api())

# Flask 서버 생성
app = Flask(__name__)

@app.route('/', methods=['GET'])
def index():
    return "flask서버 작동중"

@app.route('/redis/<string:stock_name>', methods=['GET'])
def get_stock_info_by_name(stock_name):
    # Redis에서 주식 이름에 해당하는 정보를 가져옴
    value = redis.get(stock_name)

    # 정보가 없으면 404 에러
    if value is None:
        return jsonify({"해당주식을 찾을수 없습니다."}), 404

    # JSON 형식으로 반환
    return jsonify(json.loads(value))

# 이걸로 stock엔티티에 값집어넣기 
@app.route('/redis', methods=['GET'])
def get_stock_info():

    # Redis에서 모든 키를 가져옴
    keys = redis.keys()
    
    # 각 키에 대한 정보를 가져와서 주식이름과 시장만 저장
    data = []

    for key in keys:
        value = redis.get(key)

        if value is not None:
            stock_info = json.loads(value)
            data.append({
                "stock_name": key,
                "market": stock_info['mrktCtg']
            })
        
    # JSON 형식으로 반환
    return jsonify(data)

# 서버 실행 함수
def run_server():
    app.run(host='0.0.0.0', port=4000)

def run_redis():
    print("redis 작업 시작")

    # 5분마다 한번씩 함수 실행, 테스트를 위해 30초마다 한번씩 함수 실행
    schedule.every(30).seconds.do(job_wrapper)

    while True:
        schedule.run_pending()
        time.sleep(1)

# 주식 딕션 호출 
async def stock_api():
    global cheak 

    # 코스피 또는 코스닥 선택 호출
    if cheak == True:
        cheak = False 
        market = 'KOSPI'
    else:
        cheak = True
        market = 'KOSDAQ'

    url = "공공데이터포털에서 받은 url"
    decode_key = unquote("decode_key")
    queryParams = '?' + urlencode({
        quote_plus('serviceKey') : decode_key,
        quote_plus('numOfRows') : '1000', # 원하는 갯수
        quote_plus('pageNo') : '1',
        quote_plus('resultType') : 'json',
        quote_plus('mrktCls') : market,
    })

    # 비동기 호출 
    async with aiohttp.ClientSession() as session:
        async with session.get(url+queryParams, ssl=False) as response:
            
            # 반환 성공 여부
            # 200 - 성공
            print(response.status)

            # json 형식으로 변환
            stock_data = json.loads(await response.text())
            items = stock_data["response"]["body"]["items"]["item"]

            # 파싱
            for item in items:
                stock_name = item["itmsNm"]
                stock_redis = {
                    "mrktCtg": item["mrktCtg"],
                    "clpr": item["clpr"],
                    "vs": item["vs"],
                    "fltRt": item["fltRt"],
                    "mkp": item["mkp"],
                    "hipr": item["hipr"],
                    "lopr": item["lopr"],
                    "trqu": item["trqu"],
                    "lstgStCnt": item["lstgStCnt"],
                    "mrktTotAmt": item["mrktTotAmt"]
                }

                # redis에 저장
                redis.set(stock_name, json.dumps(stock_redis))  # 딕셔너리를 JSON 문자열로 변환

# Flask 서버를 위한 프로세스 생성
flask_process = Process(target=run_server)

# Redis 작업을 위한 프로세스 생성
redis_process = Process(target=run_redis)

if __name__ == '__main__':
    # 프로세스 시작 (Flask 서버 실행)
    flask_process.start()

    # 프로세스 시작 (Redis 업데이트 작업 실행)
    redis_process.start()
