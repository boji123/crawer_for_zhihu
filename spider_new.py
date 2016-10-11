# coding:utf8
'''
author baiji
在写该爬虫之前没有python以及多线程开发经验，由于多次修改与优化，代码比较绕，还请见谅
data 2016/9/17 7:37
'''
import threading,urllib2,re
import time
import socket
import random
import copy
import os#仅linux可用
import cookielib
#---------------配置---------------
data_thread_num = 2
people_thread_num = 0
use_proxy = True
save_timer = 900
#----------------------------------
#全局socket超时
socket.setdefaulttimeout(10)
#文件锁
savelock = threading.Lock()
#输出锁
printlock = threading.Lock()
#带输出锁的输出函数
def print_with_lock(string):
    printlock.acquire()
    print(string)
    printlock.release()
	
	
#代理服务器设置
if use_proxy:
    proxyHandler = urllib2.ProxyHandler({"https" : "boji:boji@139.129.14.106:3128"})#
    opener = urllib2.build_opener(proxyHandler)
else:
    opener = urllib2.build_opener()
#http头
opener.addheaders = [
('Accept', 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8'),
('Accept-Language', 'zh-CN,zh;q=0.8'),
('Cache-Control',  'max-age=0'),
('Connection', 'keep-alive'),
('Host', 'www.zhihu.com'),
('Upgrade-Insecure-Requests','1'),
('User-Agent', 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.106 Safari/537.36'),
('Cookie', 'd_c0="ABBAs5rTXwqPTq1xOzzICZN_42dxn_lD5Yg=|1471009504"; _za=84e7cc01-9882-4d64-8f0e-723b3a5a7070; _zap=819db8a5-a703-4433-9e6a-afb363986111; q_c1=b45afcdf9e204738965a299a7bb600c3|1473647941000|1471009499000; imhuman_cap_id="ZTY5NjRjN2I5NGNjNDZhZWIzZjdlNjhkMmQyZDRjOTE=|1474358420|ce851af1795e0b063be05b4fa705b65c71ce244e"; _xsrf=592ceafb2ecad81b0e47b15ecece9069; l_cap_id="YTkyZjg1NjhmMGE3NGYzMzkwNTdmN2VkN2Y3ZGIwZmI=|1476028431|8ca3e414d2a1b7084027b3ea2685d463e4f16dbd"; cap_id="NGU3NjRiYzlkMjFlNGZkOWEwYmJhNjYzOGNlZDMwM2E=|1476028431|06c56812549c76e71954c750345e571f39e3226f"; __utmt=1; login="NDZhNTcyNjZiNTMxNDk3OThhMWFhMzc3MjZlZDdmZGQ=|1476028493|844ca7d9e59e2a4fb0b21889e5435a577448664c"; n_c=1; a_t="2.0AADAQ-IoAAAXAAAA9_UhWAAAwEPiKAAAABBAs5rTXwoXAAAAYQJVTen1IVgAOJUTufFr9EF88-J6f11z8JfUpQ9pigcZhRiMP_zGiZmE57iLNoufvg=="; __utma=51854390.609980075.1475906044.1475907388.1475999634.3; __utmb=51854390.33.9.1475999853140; __utmc=51854390; __utmz=51854390.1475907388.2.2.utmcsr=bing|utmccn=(organic)|utmcmd=organic|utmctr=%E6%9C%8D%E5%8A%A1%E5%99%A8%E5%BC%80%E7%88%AC%E8%99%ABIP%E8%A2%AB%E5%B0%81; __utmv=51854390.100--|2=registration_date=20140320=1^3=entry_date=20140320=1'),
#('Accept-Encoding','gzip, deflate, sdch, br')
] 

urllib2.install_opener(opener)

#cookie=cookielib.CookieJar()
#cookie.set_cookie('d_c0="ABBAs5rTXwqPTq1xOzzICZN_42dxn_lD5Yg=|1471009504"; _za=84e7cc01-9882-4d64-8f0e-723b3a5a7070; _zap=819db8a5-a703-4433-9e6a-afb363986111; q_c1=b45afcdf9e204738965a299a7bb600c3|1473647941000|1471009499000; imhuman_cap_id="ZTY5NjRjN2I5NGNjNDZhZWIzZjdlNjhkMmQyZDRjOTE=|1474358420|ce851af1795e0b063be05b4fa705b65c71ce244e"; _xsrf=592ceafb2ecad81b0e47b15ecece9069; l_cap_id="MzIxMTQ5ODRjNDlkNDc1YWJhMGUwZTAyOGQ5YTM2Mjk=|1475900843|bbea7244c0b0e0c93bbaff44915e4b8ecbb476a7"; cap_id="MzA1OGU4ZmQyOGZlNGYyZmIwZjFiZGZmOTY4YjhkODA=|1475900843|70df7082b463a987798ec289893df4fc491cb0fd"; __utmt=1; login="MmVlYjg5Y2IzY2EwNDI5MTgwYzllMmJmMjc5NWQ1OTY=|1475900892|3c1254f621f8ca6df9e6988500d1aa0f1d03f7ef"; n_c=1; __utma=51854390.555943539.1475723538.1475815608.1475897158.3; __utmb=51854390.4.10.1475897158; __utmc=51854390; __utmz=51854390.1475723538.1.1.utmcsr=zhihu.com|utmccn=(referral)|utmcmd=referral|utmcct=/; __utmv=51854390.100--|2=registration_date=20140320=1^3=entry_date=20140320=1; a_t="2.0AADAQ-IoAAAXAAAA6QIgWAAAwEPiKAAAABBAs5rTXwoXAAAAYQJVTd0CIFgARwp0TaA00twqsb2GqH8pCO3pcfLje06cyam1MLqgMmYHaVawPYPnlg=="')
#handler=urllib2.HTTPCookieProcessor(cookie)
#opener=urllib2.build_opener(handler)
#urllib2.install_opener(opener)
#测试代理ip用
#response = urllib2.urlopen("http://ip.catr.cn/")
#print response.read()
#-------------------------------------------------------------------------------
class CrawThread(threading.Thread):
    def __init__(self, thread_id):

        threading.Thread.__init__(self)
        self.thread_id = thread_id
        self.input_id = 0
        self.output_data = ''
        self.output_numlist = []
        self.output_peoplelist = []
        self.is_running = True
        self.url_id = 0

    def run(self):
        while(self.is_running):
            #如果有数据数据，则处理，没有则休眠		
            if(self.input_id != 0):
                #保存输入的id，这里有点绕，有时间应当重写，改成is_finish标志
                self.url_id = self.input_id
                try:
                    self.output_data, self.output_numlist, self.output_peoplelist = self.do_craw(self.input_id)
                except Exception,e:
                    print_with_lock("thread %d error :" % self.thread_id + str(Exception) + ':' + str(e))
                    self.output_data = ''
                #清空输入缓冲，表示任务已完成
                self.input_id = 0
            #休眠用于控制爬取速度			
            time.sleep(random.randint(1, 2))

    def do_craw(self, num):	
        response = urllib2.urlopen("https://www.zhihu.com/question/%d" % num)
        str_tmp = response.read()
        #去除各种空白符
        pattern1 = re.compile(r'[\t\n\r]+')
        str_tmp = pattern1.sub('', str_tmp)

        #抓取信息部分
        pattern2 = re.compile(r'<div class="zm-editable-content clearfix">(.*?)<\/div>')
        res = pattern2.findall(str_tmp)

        #抓取链接部分
        pattern3 = re.compile(r'<a class="question_link" href="/question/([0-9]*?)" data-id=')
        numlist = pattern3.findall(str_tmp)

        #抓取用户部分
        pattern4 = re.compile(r'target="_blank" href="/people/(.*?)"')
        peoplelist = pattern4.findall(str_tmp)

        return_string = ''
        for row in res:
            return_string = return_string + row

        if return_string == '':
            pattern5 = re.compile(r'限制使用')
            if pattern5.findall(str_tmp) :
                print_with_lock(str_tmp)
            print_with_lock('catch data null : %s' % num)

        return return_string, numlist, peoplelist
#-------------------------------------------------------------------------------
class NumManager(threading.Thread):
    def __init__(self):
        #读入数据到set
        self.new_numlist = set()
        numlist_file = open('save_new_numlist.txt')
        str_tmp = numlist_file.read()
        list = str_tmp.split()
        for num in list:
            self.new_numlist.add(num)

        self.old_numlist = set()
        numlist_file = open('save_old_numlist.txt')
        str_tmp = numlist_file.read()
        list = str_tmp.split()
        for num in list:
            self.old_numlist.add(num)

        print_with_lock('numlist init')
        #初始化线程控制参数
        threading.Thread.__init__(self)
        self.is_running = True
        self.is_saving = False

    def run(self):
        while(self.is_running):
            time.sleep(save_timer)
            pid = os.fork()
            if pid == 0: #复制一个子进程专用于保存
                self.numlist_save()
                return

    def numlist_save(self):
        #由于存储耗时较长，这段代码改动成输出较多，用于分析耗时
        #set遍历时其大小不得更改，所以拷贝一个副本用于存储
        self.is_saving = True
        print_with_lock('copying numlist set')
        tmp_new_numlist = copy.deepcopy(self.new_numlist)
        tmp_old_numlist = copy.deepcopy(self.old_numlist)
        #print_with_lock('numlist set copied')
        self.is_saving = False
        #将副本转化成字符串，本来认为多线程对应多核，这段可以在某个核独立完成，实际上这段代码最耗时（差一到两个数量级）
        print_with_lock('prepare new_numlist to str')
        str_tmp = ''
        for num in tmp_new_numlist:
            str_tmp = str_tmp + num + ' '
        #print_with_lock('new_numlist to str finished')
        #保存字符串
        savelock.acquire()
        #print_with_lock('saving new_numlist')
        numlist_file = open('save_new_numlist.txt', 'w')
        numlist_file.write(str_tmp)
        numlist_file.close()
        print_with_lock('new_numlist saved')
        savelock.release()
        #另一个文件如法炮制
        print_with_lock('prepare old_numlist to str')		
        str_tmp = ''
        for num in tmp_old_numlist:
            str_tmp = str_tmp + num + ' '
        #print_with_lock('old_numlist to str finished')			
			
        savelock.acquire()
        #print_with_lock('saving old_numlist')
        numlist_file = open('save_old_numlist.txt', 'w')
        numlist_file.write(str_tmp)
        numlist_file.close()
        print_with_lock('old_numlist saved')
        savelock.release()

    def add_new_num(self, num):
        if num is None:
            return False

        if num not in self.new_numlist and num not in self.old_numlist:
        #如果处于拷贝set副本的状态，等待
            while(self.is_saving == True):
                pass
            self.new_numlist.add(num)
            return True
        else:
            return False

    def add_new_nums(self, numlist):
        if numlist is None or len(numlist) == 0:
            return
        for num in numlist:
            self.add_new_num(num)

    def has_new_num(self):
        return len(self.new_numlist) != 0

    def get_new_num(self):
        if self.has_new_num():
            while(self.is_saving == True):
                pass
            new_num = self.new_numlist.pop()
            self.old_numlist.add(new_num)
            return new_num
        else:
            while(self.add_new_num(str(random.randint(20000000, 50000000))) == False):
                pass
            return False
#-------------------------------------------------------------------------------
class PeopleThread(threading.Thread):
    def __init__(self, thread_id):
        self.thread_id = thread_id
        threading.Thread.__init__(self)
        self.input_people = ''
        self.is_running = True
        self.output_numlist = []
        self.output_people = ''
    def run(self):
        while(self.is_running):
            if(self.input_people != ''):
                try:
                    self.output_people = self.input_people
                    self.output_numlist = self.do_craw(self.input_people)
                except Exception,e:
                    print_with_lock("thread people error :" + str(Exception) + ':' + str(e))
                    self.output_numlist = []
                self.input_people = ''
            time.sleep(random.randint(2, 3))

    def do_craw(self, people):
        people_response = urllib2.urlopen("https://www.zhihu.com/people/%s" % people)
        people_data = people_response.read()

        #根据人抓取链接
        people_pattern1 = re.compile(r'[\t\n\r]+')
        people_data = people_pattern1.sub('', people_data)

        people_pattern2 = re.compile(r'<a class="question_link" href="/question/([0-9]{8})')
        people_numlist = people_pattern2.findall(people_data)
        if len(people_numlist) == 0:
            print_with_lock(people_data)
            print_with_lock('catch people too fast')
            time.sleep(10)
        return people_numlist
#-------------------------------------------------------------------------------
class PeopleManager(threading.Thread):
    def __init__(self):
        self.new_list = set()
        list_file = open('save_new_peoplelist.txt')
        str_tmp = list_file.read()
        list = str_tmp.split()
        for data in list:
            self.new_list.add(data)

        self.old_list = set()
        list_file = open('save_old_peoplelist.txt')
        str_tmp = list_file.read()
        list = str_tmp.split()
        for data in list:
            self.old_list.add(data)

        print_with_lock('peoplelist init')

        threading.Thread.__init__(self)
        self.is_running = True
        self.is_saving = False

    def run(self):
        while(self.is_running):
            time.sleep(save_timer)
            pid = os.fork()
            if pid == 0: #子进程
                self.list_save()
                return

    def list_save(self):
        self.is_saving = True
        print_with_lock('copying people set')
        tmp_new_list = copy.deepcopy(self.new_list)
        tmp_old_list = copy.deepcopy(self.old_list)
        #print_with_lock('people set copied')
        self.is_saving = False

        print_with_lock('prepare new_peoplelist to str')	
        str_tmp = ''
        for data in tmp_new_list:
            str_tmp = str_tmp + data + ' '
        #print_with_lock('new_peoplelist to str finish')	
			
        savelock.acquire()
        #print_with_lock('saving new_peoplelist')
        list_file = open('save_new_peoplelist.txt', 'w')
        list_file.write(str_tmp)
        list_file.close()
        print_with_lock('new_peoplelist saved')
        savelock.release()

        print_with_lock('prepare old_peoplelist to str')	
        str_tmp = ''
        for data in tmp_old_list:
            str_tmp = str_tmp + data + ' '
        #print_with_lock('old_peoplelist to str finish')	

        savelock.acquire()
        #print_with_lock('saving old_peoplelist')
        list_file = open('save_old_peoplelist.txt', 'w')
        list_file.write(str_tmp)
        list_file.close()
        print_with_lock('old_peoplelist saved')
        savelock.release()

    def add_new(self, data):
        if data is None:
            return

        if data not in self.new_list and data not in self.old_list:
            while(self.is_saving == True):
                pass
            self.new_list.add(data)

    def add_news(self, datas):
        if datas is None or len(datas) == 0:
            return
        for data in datas:
            self.add_new(data)

    def has_new(self):
        return len(self.new_list) != 0

    def get_new(self):
        if self.has_new():
            while(self.is_saving == True):
                pass
            data = self.new_list.pop()
            self.old_list.add(data)
            return data
        else:
            return False
#-------------------------------------------------------------------------------
class SpiderMain(object):
    def __init__(self):
        self.threads = []
        self.thread_num = data_thread_num
        self.numObj = NumManager()
        self.numObj.start()

        self.peopleThreads = []
        self.people_thread_num = people_thread_num
        self.peopleObj = PeopleManager()
        self.peopleObj.start()

        #初始化线程
        for x in range(self.thread_num):
            self.threads.append(CrawThread(x))
        for x in range(self.people_thread_num):
            self.peopleThreads.append(PeopleThread(x))

        #启动抓取数据线程
        for thread in self.threads:
            thread.start()
        print_with_lock("%d thread start, waiting for mission" % self.thread_num)
        #启动抓取人物线程
        for thread in self.peopleThreads:
            thread.start()
        print_with_lock("%d people thread start, waiting for mission" % self.people_thread_num)
    def craw(self):
        while True:
            try:
                #遍历每个线程
                for thread in self.threads:
                    #如果输入缓冲为空，说明线程空闲，分配任务
                    if thread.input_id == 0:
                        if(thread.output_data != ''):
                            print_with_lock("craw %d" % thread.url_id + ' thread: %d' % thread.thread_id)
                            savelock.acquire()
                            self.data_save(thread.url_id, thread.output_data)
                            savelock.release()
                        #清空输出
                        thread.output_data = ''
                        self.numObj.add_new_nums(thread.output_numlist)
                        self.peopleObj.add_news(thread.output_peoplelist)
                        #分配新的num（新的任务）
                        new_num = self.numObj.get_new_num()
                        if(new_num != False):
                            thread.input_id = int(new_num)
                #处理抓取人的任务
                for thread in self.peopleThreads:
                    if thread.input_people == '':
                        if(len(thread.output_numlist) != 0):
                            print_with_lock("craw people : %s" % thread.output_people)
                            self.numObj.add_new_nums(thread.output_numlist)
                        self.output_numlist = []
                        new_people = self.peopleObj.get_new()
                        if(new_people != False):
                            thread.input_people = new_people
        #没有写退出处理,只能强制中止
        #-----
        #-----
            except Exception,e:
                print_with_lock("craw failed" + str(Exception) + ":" + str(e))
        #没有写退出处理,只能强制中止
        #-----
        #-----
        while(len(self.threads) != 0):
            for x in range(len(self.threads)):
                thread = self.threads[x]
                if(thread.input_id == 0):
                    if(thread.output_data != ''):
                        print_with_lock("craw %d" % thread.url_id)
                        self.data_save(thread.url_id, thread.output_data)

                    thread.is_running = False
                    del self.threads[x]
                    break
        self.peopleThread.is_running = False
        print_with_lock("mission end")

    def data_save(self, url_id, data):
        fout = open("file/file_%d.html" % (url_id), "w")
        fout.write('<meta charset="UTF-8">')
        fout.write(data)

if __name__=="__main__":
    spider = SpiderMain()
    spider.craw()
