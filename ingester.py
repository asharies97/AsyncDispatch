import asyncio
import time

import os,json,sys
from dotenv import load_dotenv
load_dotenv()
from auth import TokenService
import aiohttp,aiofiles
import certifi
import ssl
ssl_context = ssl.create_default_context(cafile=certifi.where())

###Get the Environment variables and other Hyperparameters
MAX_QUEUE_SIZE=2000
MAX_PARALLEL_REQUESTS=200
MAX_ERRORS=100
MAX_RETRY_COUNT=2
METHOD=os.getenv("DEFAULT_METHOD")
API_URL=os.getenv("API_HOST")
RETRY_LIMIT=int(os.getenv("RETRY_LIMIT"))
AUTH_ACCOUNT=os.getenv("AUTH_ACCOUNT")
token_service = TokenService()
TOKEN=token_service.get_token(AUTH_ACCOUNT)
print(f'Token acquired successfully')

input_file=sys.argv[1]
service=sys.argv[2]

async def write_to_jsonl(file_path, data):
    async with aiofiles.open(file_path, mode='a', encoding='utf-8') as f:
        # json.dumps converts dict to string, then we add a newline
        line = json.dumps(data) + '\n'
        await f.write(line)


async def get_method(session,METHOD):
    return {
        "POST":session.post,
        "PUT":session.put,
        "PATCH":session.patch,
        "DEL":session.delete,
    }[METHOD]

def get_headers():
    headers = {}
    token = TOKEN
    headers["Authorization"] = f"Bearer {token}"
    headers["Accept"] = "application/json"
    return headers
    

async def process_record(request_method,record,retry_count=0):
    try:
        while retry_count<=RETRY_LIMIT:
            retry_count+=1
            if METHOD=="DEL":
                request_url=API_URL+f"/v1/{service}/{record['id']}"
                response=await request_method(request_url,
                                        headers=get_headers())
            else:
                request_url=f"{API_URL}/v1/{service}"
                response=await request_method(request_url,
                                        headers=get_headers(),
                                        json=record)
                
            if response.status<400 or retry_count>RETRY_LIMIT:
                return response
            else:
                continue
    except Exception as e:
            print( {record['id']:e,'record':record})


async def producer(data_queue):
    try:
        with open(input_file) as f:
            for line in f:
                await data_queue.put(json.loads(line))
    except Exception as e:
        print(f"Exception is {e}")


async def worker(id, data_queue, request_method, result_collector, stop_event):
    while not stop_event.is_set():
        try:
            # Use a timeout to allow the worker to check stop_event.is_set() regularly
            record = await asyncio.wait_for(data_queue.get(), timeout=1.0)
        except asyncio.TimeoutError:
            continue

        try:
            if record is None:
                break
            
            request_response = await process_record(request_method, record, 0)
            if request_response and request_response.status >= 400:
                rsp = await request_response.json()
                error_msg = rsp.get('error') or rsp.get('message') or "Unknown Error"
                await result_collector.put({record['id']: error_msg})
                
                if result_collector.qsize() >= MAX_ERRORS:
                    print(f"!!! Error limit reached. Signalling stop.")
                    stop_event.set()
                    break
                    
        except Exception as e:
            print(f"Worker {id} encountered error: {e}")            
        finally:
            data_queue.task_done()


async def main():
    data_queue = asyncio.Queue(maxsize=MAX_QUEUE_SIZE)
    result_collector = asyncio.Queue(maxsize=MAX_ERRORS + 10) # Buffer to prevent blocking
    stop_event = asyncio.Event()
    
    connector = aiohttp.TCPConnector(limit=MAX_PARALLEL_REQUESTS, ssl=ssl_context)
    async with aiohttp.ClientSession(connector=connector) as session:
        request_method = await get_method(session, METHOD)
        
        workers = [
            asyncio.create_task(worker(f"worker-{i}", data_queue, request_method, result_collector, stop_event))
            for i in range(MAX_PARALLEL_REQUESTS)
        ]
        
        # Start producer as a task so we can monitor/cancel it
        producer_task = asyncio.create_task(producer(data_queue))

        queue_join_task = asyncio.create_task(data_queue.join())
        stop_event_task = asyncio.create_task(stop_event.wait())

        # Wait for either completion or the stop event
        done, pending = await asyncio.wait(
            [queue_join_task, stop_event_task],
            return_when=asyncio.FIRST_COMPLETED
        )
        
        # Immediately cancel everything once the limit is hit or work is done
        stop_event.set() 
        producer_task.cancel()
        for w in workers:
            w.cancel()
        for task in pending:
            task.cancel()
        
        # Allow a moment for tasks to acknowledge cancellation
        await asyncio.gather(*workers, producer_task, return_exceptions=True)

        # Write results to file
        async with aiofiles.open('failed_'input_file.split('.jso')[0]+'.jsonnl', mode='w', encoding='utf-8') as f:
            while not result_collector.empty():
                item = await result_collector.get()
                await f.write(json.dumps(item) + '\n')
                result_collector.task_done()
    
    print(f"Process Completed")

if __name__=="__main__":
    try:
        start = time.time()
        asyncio.run(main())
        end = time.time()

        print(f"Runtime: {end - start:.4f} seconds")
    except Exception as e:
        print(f'Processing failed at main with exception as {e}')