import asyncio
import argparse
import aiohttp
from lxml import etree
import urllib3
from concurrent.futures import ThreadPoolExecutor

# 禁用安全请求警告,当目标使用https时使用
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

_session:aiohttp.client.ClientSession | None = None

def args():
    '''命令行参数设置'''
    parser = argparse.ArgumentParser(prog = 'crawler-v2', description = 'Crawler used for search whole book')
    #爬取书籍与章节的有关信息
    parser.add_argument("URl", help= "url to crawl--https://www.sdmz.net/index.html")
    parser.add_argument("-s", "--start", default = 0, help = "start index", type = int)
    parser.add_argument("-e", "--end", default = 10, help = "end index", type = int)
    #存储路径
    parser.add_argument("-p", "--path", default ="data/", help = "path to save data", type = str)
    #程序性能方面--多线程
    parser.add_argument("-mgw", "--max_get_workers", default = 5, type = int)
    parser.add_argument("-mpw", "--max_parser_workers", default = 5, type = int)

    return parser.parse_args()

#获取
async def get_html(url_queue:asyncio.Queue, raw_queue:asyncio.Queue):
    session = get_session()
    while True:
        url:str = await url_queue.get()
        if url is None:
            url_queue.task_done()
            break

        try:
            async with session.get(url) as response:

                if response.status != 200:
                    await raw_queue.put({
                        "url": url,
                        "code": response.status,
                        "raw_content": None,
                        "error": f"HTTP {response.status}"
                    })
                    continue

                content = await response.text(encoding="utf-8")

                await raw_queue.put({
                    "url": url,
                    "code": response.status,
                    "raw_content": content,
                    "error": None
                })
        except asyncio.TimeoutError:
            await raw_queue.put({
                "url": url,
                "code": -1,
                "raw_content": None,
                "error": "request timeout"
            })
        except aiohttp.ClientConnectorError as e:
            await raw_queue.put({
                "url": url,
                "code": -2,
                "raw_content": None,
                "error": f"connection error: {e}"
            })
        except aiohttp.ClientError as e:
            await raw_queue.put({
                "url": url,
                "code": -3,
                "raw_content": None,
                "error": f"aiohttp error: {e}"
            })

        except Exception as e:
            await raw_queue.put({
                "url": url,
                "code": -4,
                "raw_content": None,
                "error": f"unknown error: {e}"
            })
        finally:
            url_queue.task_done()


def get_session() -> aiohttp.ClientSession:
    global _session
    if _session is None or _session.closed:
        _session = aiohttp.ClientSession(
            connector = aiohttp.TCPConnector(
                limit = 200,
                limit_per_host = 50,
                ttl_dns_cache = 300,
                enable_cleanup_closed = True,
            ),
            timeout = aiohttp.ClientTimeout(total = 30, connect = 5),
        )
    return _session

async def close_session() -> None:
    global _session
    if _session and not _session.closed:
        await _session.close()
        _session = None


#解析
async def get_parser_content(raw_queue:asyncio.Queue,
                             result_queue:asyncio.Queue,
                             parser_loop:asyncio.AbstractEventLoop,
                             executor:ThreadPoolExecutor):
    while True:
        item = await raw_queue.get()

        if item is None:
            raw_queue.task_done()
            break
        try:
            if item["code"] != 200:
                continue

            head, content = await parser_loop.run_in_executor(
                executor,
                parser,
                item["raw_content"]
            )

            await result_queue.put({
                "url": item["url"],
                "head": head,
                "content": content,
            })

        finally:
            raw_queue.task_done()


def parser(text:str):

    #解析
    root = etree.HTML(text)
    if root == None :
        raise ValueError("HTML 解析失败")

    #获取标题
    head = root.xpath("/html/body/main/article[@class='chapter']/h1[@class='chapter-title']/text()")
    head_copy = head[0].replace("\u3000", " ")

    #获取内容
    content = root.xpath("/html/body/main/article[@class='chapter']/div[@class='chapter-text' and @id='chapter-text']/p/text()")
    content_copy = ''
    for i in content:
        content_copy += i

    return head_copy, head_copy + '\n' + content_copy

#存储
async def write(result_queue:asyncio.Queue, path:str, loop, executor:ThreadPoolExecutor):
    while True:
        item = await result_queue.get()
        if item is None:
            break
        filepath = path + item['head'] + ".txt"
        # 阻塞写文件丢线程池，别卡事件循环
        await loop.run_in_executor(executor, _sync_write, filepath, item['content'])
        result_queue.task_done()

def _sync_write(filepath, content):
    with open(filepath, "w", encoding="utf-8") as f:
        f.write(content)

async def main():

    arg = args()


    raw_queue = asyncio.Queue(200)
    result_queue = asyncio.Queue(200)

    url_queue = asyncio.Queue(200)
    for i in range(arg.start, arg.end + 1):
        await url_queue.put(
            arg.URl + f'{i:03d}' + '.htm'
        )

    executor = ThreadPoolExecutor(max_workers = arg.max_parser_workers)
    parser_loop = asyncio.get_running_loop()
    write_loop = asyncio.get_running_loop()

    parser_tasks = [
        asyncio.create_task(
            get_parser_content(raw_queue, result_queue, parser_loop, executor)
        )
        for _ in range(arg.max_parser_workers)]

    write_task = asyncio.create_task(write(result_queue, arg.path, write_loop, executor))

    get_tasks = [
        asyncio.create_task(
            get_html(url_queue, raw_queue)
        )
        for _ in range(arg.max_get_workers)
    ]

    for i in range(arg.max_get_workers):
        await url_queue.put(None)
    await asyncio.gather(*get_tasks)

    for i in range(arg.max_parser_workers):
        await raw_queue.put(None)
    await asyncio.gather(*parser_tasks)

    await result_queue.put(None)
    await write_task



    await close_session()
    executor.shutdown(wait=True)

    print("done")

if __name__ == '__main__':
    asyncio.run(main())

