#!/usr/bin/env python

"""downloadManager.py: lightweight threaded bulk download library

Define one or more callback functions whose parameters accept the html response
in as a first parameter, and optionally a response object to receive the return
value of urlopen

Usage:
    def callback(html, response):
        print html

    dm = downloadManager()
    dm.download(url, callback)
    dm.wait()

"""

# Import builtin modules
import urllib, urllib2, json, Queue, time, sys, traceback, os
from hashlib import md5
from threading import Thread, Lock
from cookielib import CookieJar





__author__ = "Chris Dobson"
__copyright__ = "Copyright 2015, 8906386 CANADA LIMITED"
__credits__ = ["Chris Dobson"]
__license__ = "Apache License Version 2.0"
__version__ = "1.0.0"
__maintainer__ = "Chris Dobson"
__email__ = "chris@penguinleaf.com"
__status__ = "Development"


class downloadManager:
    class downloadItem:
        def __init__(self, url, callback, time, dm=None):
            self.url = url
            self.callback = callback
            self.time = time
            self.dm = dm

        def run(self, opener):
            self.dm._scheduleLock.acquire()
            mytimeslot = self.dm._lastDownload + self.dm._throttle    
            self.dm._lastDownload = mytimeslot
            # print "Waiting %0.3f" % (self.time - time.time())
            self.dm._scheduleLock.release()
            now = time.time()
            time.sleep(max(mytimeslot, now) - now)
            response = opener.open(self.url)
            html = response.read()
            if(self.dm._verbose): print "Downloading: %s" % self.url
            if self.dm._cache_dir:
                cached_file = open(os.path.join(self.dm._cache_dir, md5(self.url).hexdigest()),'w')
                cached_file.write(html)
            try:
                self.callback(response, html)
            except:
                if(self.dm._verbose): print "\nError occured in user callback for %s" % self.url
                traceback.print_exc(file=sys.stdout)
            

    def __init__(self, domain="http://localhost", maxThreads=10, throttle=100, verbose=False):
        self._cj = CookieJar()
        self._opener = urllib2.build_opener(urllib2.HTTPCookieProcessor(self._cj))
        self._queue = Queue.Queue()
        self._maxThreads = maxThreads
        self._workers = []
        self._throttle = throttle * 0.001
        self._start = time.time()
        self._nextDownload = time.time()
        self._lastDownload = 0
        self._verbose = verbose
        self._queueingLock = Lock()
        self._scheduleLock = Lock()
        self.alive = True
        self._has_started = False
        self._backoff_factor = 3
        self._backoff_time = 0
        self.domain = domain
        self._cache_dir = "./.web_cache"
        if not os.path.isdir(self._cache_dir):
            os.makedirs(self._cache_dir)
        def _worker(dm):
            while dm.alive:
                di = None
                try:
                    di = dm._queue.get(False)
                    if dm._cache_dir and os.path.isfile(os.path.join(dm._cache_dir, md5(di.url).hexdigest())):
                        cached_file = open(os.path.join(dm._cache_dir, md5(di.url).hexdigest()),'r')
                        di.callback(None, cached_file.read())
                    else:
                        if(self._backoff_time < time.time()):
                            di.run(dm._opener)    
                        else:
                            time.sleep(di.time - time.time())
                            di2 = downloadManager.downloadItem(di.url, di.callback, dm._nextDownload, dm)
                            dm._queue.put(di2)
                    dm._queue.task_done()
                except Queue.Empty:
                    # print "Queue Empty"
                    time.sleep(0.2)
                except urllib2.HTTPError:
                    dm._backoff_time = time.time() + dm._backoff_factor
                    dm._backoff_factor *= 2
                    dm._backoff_factor += 2
                    dm._cj = CookieJar()
                    di2 = downloadManager.downloadItem(di.url, di.callback, dm._nextDownload, dm)
                    dm._queue.put(di2)





        for i in range(maxThreads):
            worker = Thread(target=_worker, args=(self,))
            worker.setDaemon(True)
            worker.start()
            self._workers.append(worker)

        def _monitor(self):
            if(self._verbose):time.sleep(5)
            while self.alive or not self._has_started:
                msg = " Remaining: %i items. ETA: %s ( %i minutes )" %  (self._queue.qsize(), time.strftime("%a, %H:%M:%S", time.localtime(time.time()+ self._queue.qsize() * self._throttle)), (self._queue.qsize() * self._throttle)/60)
                msg += len(msg) * "\b"
                sys.stdout.write(msg)
                sys.stdout.flush()
                time.sleep(.2)
                # if(not self._verbose): time.sleep(29)
            self.alive = False

        self._monitor = Thread(target=_monitor, args=(self,))
        self._monitor.setDaemon(True)
        self._monitor.start()

    def download(self, url, callback):
        self.alive = True
        if url.startswith("/"):
            url = self.domain + url
        if not (url.startswith("http://") or url.startswith("https://")):
            url = self.domain + "/" + url
        
        
        if(self._queueingLock.acquire()):
            di = downloadManager.downloadItem(url, callback, self._nextDownload, self)
            self._nextDownload = max(self._nextDownload, time.time()) + self._throttle
            self._queueingLock.release()
            if self._verbose: print "Queueing %s, %i already in queue" % (url, self._queue.qsize())
            self._queue.put(di)
            self._has_started = True
        

    def run(self):    
        # print "Running..."
        try:
            while not self._queue.empty():
                time.sleep(10)
                if self._queue.empty():
                    time.sleep(60)
                    if self._queue.empty():
                        break
        except KeyboardInterrupt:
            print "Terminated with %i incomplete downloads" % self._queue.qsize()
        self.alive = False
