import requests
from lxml import html
import re
import json
import xlwt
from queue import Queue
import threading
from xlutils.copy import copy
import xlrd
import time

HEADER = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.122 Safari/537.36'
}


class Procuder(threading.Thread):
    def __init__(self, page_queue, img_queue, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.page_queue = page_queue
        self.img_queue = img_queue

    def run(self):
        while True:
        # 判断生产结束的条件
            if self.page_queue.empty():
                break
            url = self.page_queue.get()
            self.get_detail_urls(url)
            # 休眠0.5秒防止太快
            time.sleep(0.5)
        # 输出生产者中的个数，便于统计
        print(self.img_queue.qsize())

    def get_detail_urls(self, url):
    # 获取每一页的职位信息的url
        reponse = requests.get(url, HEADER, stream=True)
        data = reponse.content.decode('gbk')
        htmls = html.etree.HTML(data)
        detail_urls = htmls.xpath('//*[@id="resultList"]/div[@class="el"]/p/span//@href')
        self.parse_datail_page(detail_urls)

    def parse_datail_page(self, detail_urls):
    # 遍历每一条职位数据，储存入列表
        for urls in detail_urls:
            new = []
            reponse = requests.get(urls, HEADER, stream=True)
            # 页面中有无法识别的内容，要加参数errors='ignore'
            data = reponse.content.decode('GBK', errors='ignore')
            htmls = html.etree.HTML(data)
            # 此网站中有些职位不属于51job的内容，这里我们把它们剔除
            try:
                name = htmls.xpath(
                    '//body//div[@class="tCompanyPage"]//div[@class="tHeader tHjob"]//div[@class="in"]//div[@class="cn"]/h1/@title')[
                    0]
            except:
                new.append('非正规网站职业')
                new.append('')
                new.append('')
                new.append('')
                new.append('')
                new.append('')
                self.img_queue.put(new)
                reponse.close()
                continue
            city = re.findall(r'<p class="msg ltype" title="(.*?)&', data, re.S)[0]
            company = htmls.xpath('//div[@class="cn"]//p[@class="cname"]//a/@title')[0]
            try:
                money = re.findall(
                    r'<body>.*?<div class="tCompanyPage".*?<div class="tHeader tHjob">.*?<div class="cn">.*?<strong>(.*?)</strong>',
                    data, re.S)[0]
            except:
                money = ''
            content = htmls.xpath('//div[@class="bmsg job_msg inbox"]//text()')
            # ensure_ascii=False表示不对中文进行转译
            content = json.dumps(content, ensure_ascii=False)
            # 将数据中不需要的内容剔除
            content = re.sub("^\\[|\\]$", "", content)
            content4 = content.replace('\\n ', '')
            content5 = content4.replace('\\r', '')
            content = content5.replace('微信分享', '')
            content = re.sub('"| |,', "", content)
            new.append(name)
            new.append(money)
            new.append(city)
            new.append(urls)
            new.append(company)
            new.append(content)
            self.img_queue.put(new)
            # 爬取完一组信息后，要及时关闭文件
            reponse.close()


class Consumer(threading.Thread):
    def __init__(self, page_queue, img_queue, gLock, a, x, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.page_queue = page_queue
        self.img_queue = img_queue
        self.gLock = gLock
        self.a = a
        self.tt = x

    def run(self):
        xx = 1
        while True:
        # 这里延迟10秒，防止因为网络延迟而异常终止
            if self.img_queue.empty() and self.page_queue.empty():
                time.sleep(10)
            if self.img_queue.empty() and self.page_queue.empty():
                break
            ddd = self.img_queue.get()
            # 上锁，因为要修改数据内容
            self.gLock.acquire()
            self.export_excel(ddd, xx)
            self.gLock.release()
            xx += 1

    def export_excel(self, next, xx):
        s = '2020年广州{}职位{}条数据'.format(self.a, self.tt)
        k = 'D:\\表格\\'
        # 打开之前的储存表头信息的excel，将数据放入
        oldWb = xlrd.open_workbook(k + s + '.xls')
        # 将上一次文件中的内容复制，这样就不会覆盖原有内容
        newWb = copy(oldWb)
        newWs = newWb.get_sheet(0)
        for j, col in enumerate(next):
            newWs.write(xx, j, col)
        newWb.save(k + s + '.xls')
        print(xx)


def main():
    gLock = threading.Lock()
    page_queue = Queue(400)
    img_queue = Queue(20000)
    # 需要爬取的职位
    i = ['golang','python']
    for a in i:
    # 预处理
        url = "https://search.51job.com/list/030200,000000,0000,00,9,99,{},2,1.html?".format(a)
        reponse = requests.get(url, HEADER)
        data = reponse.content.decode('gbk')
        # 获取一共多少条职位数据
        x = re.findall(r'<div class="dw_tlc">.*?<div class="sbox">.*?共(.*?)条职位', data, re.S)[0]
        # 获取一共多少页
        s = \
            re.findall(r'<div class="dw_page">.*?<div class="p_in">.*?</ul>.*?<span class="td">共(.*?)页，到第</span>', data,
                       re.S)[
                0]
        # 转为数字类型
        gg = int(s)
        # 先创建一个含表头的excel文件
        f = '2020年广州{}职位{}条数据'.format(a, x)
        k = 'D:\\表格\\'
        books = xlwt.Workbook(encoding="utf-8")
        sheet = books.add_sheet("广州")
        order = ['names', 'money', 'city', 'url', 'company', 'content']
        for j, col in enumerate(order):
            sheet.write(0, j, col)
        books.save(k + f + '.xls')
        # 获取每一页的url
        for aa in range(1, gg + 1):
            url = 'https://search.51job.com/list/030200,000000,0000,00,9,99,{},2,{}.html?'.format(a, aa)
            page_queue.put(url)
        # 4个生产线程
        for xx in range(4):
            t = Procuder(page_queue, img_queue)
            t.start()
        # 一个线程写入excel文件，防止文件内容被覆盖，导致出错
        t = Consumer(page_queue, img_queue, gLock, a, x)
        t.start()


if __name__ == '__main__':
    main()
