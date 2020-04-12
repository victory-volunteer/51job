import requests
from lxml import html
import re
import json
from queue import Queue
import threading
import time
import jieba
from wordcloud import WordCloud

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
            if self.page_queue.empty():
                break
            url = self.page_queue.get()
            self.get_detail_urls(url)
            # 休眠0.5秒防止太快
            time.sleep(0.5)
        print(self.img_queue.qsize())

    def get_detail_urls(self, url):
        reponse = requests.get(url, HEADER, stream=True)
        data = reponse.content.decode('gbk')
        htmls = html.etree.HTML(data)
        # 获取每一页中每一个职位信息url
        detail_urls = htmls.xpath('//body//div[@class="dw_wp"]//div[@class="dw_table"]/div[@class="el"]/p/span//@href')
        self.parse_datail_page(detail_urls)

    def parse_datail_page(self, detail_urls):
        for urls in detail_urls:
        # 爬取职位信息数据
            reponse = requests.get(urls, HEADER, stream=True)
            data = reponse.content.decode('GBK', errors='ignore')
            htmls = html.etree.HTML(data)
            content = htmls.xpath('//div[@class="bmsg job_msg inbox"]//text()')
            # ensure_ascii=False使汉字不进行转译
            content = json.dumps(content, ensure_ascii=False)
            # 去除不必要的字符
            content = re.sub("^\\[|\\]$", "", content)
            content4 = content.replace('\\n ', '')
            content5 = content4.replace('\\r', '')
            content = content5.replace('微信分享', '')
            content = re.sub('"| |,', "", content)
            self.img_queue.put(content)
            # 爬取完一个就要关闭
            reponse.close()


class Consumer(threading.Thread):
    def __init__(self, page_queue, img_queue, gLock, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.page_queue = page_queue
        self.img_queue = img_queue
        self.gLock = gLock

    def run(self):

        while True:
        # 这里休眠5秒，防止因为网络问题而出错
            if self.img_queue.empty() and self.page_queue.empty():
                time.sleep(5)
            if self.img_queue.empty() and self.page_queue.empty():
                break
            ddd = self.img_queue.get()
            self.gLock.acquire()
            # 在修改文件之前要上锁
            self.export_excel(ddd)
            self.gLock.release()

    def export_excel(self, next):
    # 将职位信息保存到txt文件
        with open("D:\\表格\\职位.txt", "a", encoding="utf-8")as f:
            f.write(next)


def main():
    gLock = threading.Lock()
    page_queue = Queue(400)
    img_queue = Queue(20000)
    # 需要爬取职位信息的城市
    i = ['200200', '020000']
    for a in i:
    # 用于找出基本信息
        url = "https://search.51job.com/list/{},000000,0000,00,9,99,python,2,1.html?".format(a)
        reponse = requests.get(url, HEADER)
        data = reponse.content.decode('gbk')
        # 获取职位数量
        x = re.findall(r'<div class="dw_tlc">.*?<div class="sbox">.*?共(.*?)条职位', data, re.S)[0]
        # 获取页数
        s = \
            re.findall(r'<div class="dw_page">.*?<div class="p_in">.*?</ul>.*?<span class="td">共(.*?)页，到第</span>', data,
                       re.S)[
                0]
        # 将页数int成为数字类型
        gg = int(s)
        # 遍历每一页放到队列中
        for aa in range(1, gg + 1):
            url = 'https://search.51job.com/list/{},000000,0000,00,9,99,golang,2,{}.html?'.format(a, aa)
            page_queue.put(url)
        # 建立多个线程
        for xx in range(4):
            t = Procuder(page_queue, img_queue)
            t.start()
        for xx in range(4):
            t = Consumer(page_queue, img_queue, gLock)
            t.start()


def run():
    # 方法一：自定义词典
    # file_userdict = 'D:\\多线程爬虫\\文本.txt'
    # jieba.load_userdict(file_userdict)
    # 读取职位信息存储文件
    f = open('D:\\表格\\职位.txt', encoding='utf-8')
    txt = f.read()
    f.close()
    fuhao = ['Flask', 'Django', 'Tornado', '高并发', '异步', '多线程', '多进程', '爬虫', '数据结构', '算法', 'ACM', '本科', 'Redis', 'Mysq',
             'Nosql', 'MongoDB', 'Linux', 'Angular', '框架', '前端', 'Storm', 'HBase', 'hadoop', 'Hive', 'Robot', 'Vue.js',
             'bug', 'Al', 'Redis', 'TDD', 'Odoo', '爬', 'html']
    # 方法二：使用add_word增加向jieba库增加新的单词
    for a in fuhao:
        jieba.add_word(a)
    # 分词
    words = jieba.lcut(txt)
    counts = {}
    # 排除除关键字之外的其他字符
    for word in words:
        if word in fuhao:
        # 统计关键字个数
            counts[word] = counts.get(word, 0) + 1
        else:
            continue
    # 将字典转为列表
    items = list(counts.items())
    # 按key值排序
    items.sort(key=lambda x: x[1], reverse=True)
    co = {}
    sm = len(items)
    print(sm)
    # 输出关键字排名
    for i in range(sm):
        word, count = items[i]
        print('{0:<10}{1:>5}'.format(word, count))
        co[word] = count
    # 将排名写为词云
    words = WordCloud(font_path="D:\\pythonIDLE\词云\\msyh.ttf", width=600, height=600, max_words=sm,
                      background_color='white').generate_from_frequencies(co)
    # 保存词云图片
    words.to_file("D:\\图片\\python职位关键字.png")


if __name__ == '__main__':
    main()
    run()
