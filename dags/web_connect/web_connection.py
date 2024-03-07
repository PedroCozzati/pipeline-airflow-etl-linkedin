from bs4 import BeautifulSoup
import requests
import urllib.parse

class WebConnection:
    headers = {
    'method':'GET',
    'path':'/dist/header/2207.4956bcf4ac05e0d43000.css',
    'scheme':'https',
    'Accept':'text/css,*/*;q=0.1',
    'Accept-Encoding':'gzip, deflate, br',
    'Accept-Language':'pt-BR,pt;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6',
    'Sec-Ch-Ua':'"Not A(Brand";v="99", "Microsoft Edge";v="121", "Chromium";v="121"',
    'Sec-Ch-Ua-Mobile': '?0',
    'Sec-Ch-Ua-Platform':"Windows",
    'Sec-Fetch-Dest':'style',
    'Sec-Fetch-Mode':'cors',
    'Sec-Fetch-Site':'same-site',
    "Connection": "keep-alive",
    'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36 Edg/121.0.0.0',
    }
    
    def __init__(self, url, keyword, estado,cidade,pais,distance,driver):
        self.url = url
        self.keyword = keyword
        self.estado = estado
        self.cidade = cidade
        self.pais = pais
        self.distance = distance
        self.driver = driver
    
    def start_connection(self):
        
        location = urllib.parse.quote(self.estado+self.cidade+self.pais)
        distance = self.distance
        driver = self.driver
        cargo = urllib.parse.quote(self.keyword)
        complete_url = self.url+'search?keywords='+cargo+'&location='+location+'&trk=public_jobs_jobs-search-bar_search-submit&geoId=104746682&distance='+distance+'&position=1&pageNum=0'
        
        driver.get(complete_url)
        
        return driver.page_source
        