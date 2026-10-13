import requests
from lxml import etree
from collections import deque
from urllib.parse import urlunparse, urlparse

#url队列
url_queue = deque()

#已访问集合
set = set()

session = requests.Session()

max_entry = 3

def get_html(url):
    headers = {
        "User-Agent": "Mozilla/5.0 (Linux; Android 16; Pixel 10) AppleWebKit/537.36 (KHTML, like Gecko) Edg/154.0.0.0 Mobile Safari/537.36"
    }
    for i in range(1, max_entry+1):
        try:
            response = session.get(url, headers=headers, timeout=(8, 12))
            if response.status_code == 200:
                response.encoding = 'utf-8'
                return response.text
        except requests.exceptions.RequestException as e:
            print(e.__class__.__name__)
            continue
    return None

#从页面中提取url
def get_urls(text, original_url):
    root = etree.HTML(text)
    if root == None :
        raise ValueError("解析错误")
    raw_urls = root.xpath("//a@href")
    parsed_original_url = urlparse(original_url)
    for raw_url in raw_urls:
        url_component = parsed_original_url._replace(path = raw_url)
        url = urlunparse(url_component)
        url_queue.append(url)

#解析内容
def get_content(text):
    root = etree.HTML(text)
    if root == None :
        raise ValueError("HTML 解析失败")

    #获取标题
    head = root.xpath("/html/body/main/article[@class='chapter']/h1[@class='chapter-title']/text()")
    head_copy = head[0].replace("\u3000", " ")
    print(head_copy)

    #获取内容
    content = root.xpath("/html/body/main/article[@class='chapter']/div[@class='chapter-text' and @id='chapter-text']/p/text()")
    content_copy = ''
    for i in content:
        content_copy += i
    print(content_copy)

    return head_copy, head_copy + '\n' + content_copy

def save_content(head, content):
    file_path = "data/" + head + ".txt"
    with open(file_path,"w",encoding="utf-8") as f:
        print(content)
        f.write(content)

def draw_graph(url):
    pass

def main():
    pass
