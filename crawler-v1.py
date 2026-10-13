import requests
from lxml import etree
import urllib3

# 禁用安全请求警告,当目标使用https时使用
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

max_entries = 3

timeout = (8, 12)

URL = "https://www.sdmz.net/hlm/001.htm"

def get_content(url):
    '''发送请求，获取响应'''
    headers = {
        "User-Agent" : "Mozilla/5.0 (Linux; Android 16; Pixel 10) AppleWebKit/537.36 (KHTML, like Gecko) Edg/154.0.0.0 Mobile Safari/537.36"
    }
    response = requests.get(url, headers=headers, timeout=timeout)
    response.encoding = "utf-8"
    return response.text

def analyze(text:str):
    '''对请求返回的文本进行解析'''

    #解析
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

def save_into_file(head,content):
    '''将爬取的内容保存文件'''
    file_path = "data/" + head + ".txt"
    with open(file_path,"w",encoding="utf-8") as f:
        print(content)
        f.write(content)

if __name__ == "__main__":
    head, content = analyze(get_content(URL))
    save_into_file(head,content)
