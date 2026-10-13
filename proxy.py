import requests
import time


'''
简单的代理ip池
从第三方ip代理商获取代理ip
api接口为：https://share.proxy.qg.net/get?key=I4TL0QBM&num=1&area=&isp=0&format=json&distinct=false
返回数据为json格式：
{
  "code": "SUCCESS",
  "data": [
    {
      "proxy_ip": "111.170.77.101",
      "server": "111.170.77.101:12171",
      "area_code": 421200,
      "area": "湖北省咸宁市",
      "isp": "电信",
      "deadline": "2026-10-13 19:06:57"
    }
  ],
  "request_id": "156da35e-9c31-4ebb-9946-60d3533dc461"
}
'''

class IPPool():
    def __init__(self, test_url, url:str = 'https://share.proxy.qg.net/get?key=I4TL0QBM&num=1&area=&isp=0&format=json&distinct=false', num:int = 20):
        self.url = url
        self.num = num
        self.ip_list = []

        count = 0
        while count < num:
            server = self.get_server()
            if server == None:
                continue
            if self.test(test_url, server):
                count += 1



    def get_server(self):
        try:
            response = requests.get(self.url, timeout = (8, 12))
            information = response.json()
            if information['code'] == 'SUCCESS':
                server = information['data']['server']
                return server
        except requests.exceptions.RequestException as e:
            print(e)
            return None
        except Exception as e:
            print(e)
            return None

    def test(self, test_url:str, server):
        proxies = {
            'http' : f'http://{server}',
            'https' : f'https://{server}'
        }
        headers = {
            'User-Agent' : 'Mozilla/5.0 (Linux; Android 16; Pixel 10) AppleWebKit/537.36 (KHTML, like Gecko) Edg/155.0.0.0 Mobile Safari/537.36'
        }
        try:
            response = requests.get(test_url, timeout = (8, 12), headers=headers, proxies = proxies)
            if response.status_code == 200:
                self.ip_list.append(server)
                return True
        except requests.exceptions.RequestException as e:
            print(e)
            return False
        except Exception as e:
            print(e)
            return False

