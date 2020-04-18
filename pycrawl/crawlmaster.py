from multiprocessing import Process, Event
from importlib import import_module
from flask import Flask, request
from common import isint
import ujson as json
import subprocess
import databaser
import logger
import badger
import time
import sys
import os


# fetch working directory
with open('/var/local/kheina.com-dir') as cwd :
	os.chdir(cwd.read().strip())


app = Flask(__name__)
logger.start('crawlers')

crawlers = { }
crawlersinfo = { }
shutdowns = { }

def startNewCrawl(modulePath, *args, **kwargs) :
	package, module = modulePath.rsplit('.', 1)
	crawler = getattr(import_module(package), module)
	crawler = crawler(*args, **kwargs)
	crawler.run()

def updateCrawlersInfo() :
	for key, crawler in crawlers.items() :
		if crawler.is_alive() :
			crawler['uptime'] = time.time() - crawler['starttime']
			crawler['status'] = 'gracefully shutting down' if shutdowns[key].is_set() else 'alive'
		else :
			crawler['status'] = 'dead'

@app.route('/start', methods=['POST'])
def start() :
	crawler = request.json.pop('crawler')
	kwargs = request.json

	try :
		crawlerid = max(key for key in crawlers.keys() if isint(key) is not None) + 1
	except (ValueError, TypeError) :
		crawlerid = 0

	if crawlerid in crawlers :
		raise AttributeError(f'id {crawlerid} already in crawlers')

	shutdowns[crawlerid] = Event()

	c = Process(target=startNewCrawl, args=(crawler,), kwargs={ 'event': shutdowns[crawlerid], **kwargs })
	c.start()

	crawlers[crawlerid] = c
	crawlersinfo[crawlerid] = { 'module': crawler, 'starttime': time.time(), 'status': 'alive', **kwargs }

	updateCrawlersInfo()
	return f'{json.dumps({ "started": crawlerid, "crawlers": crawlersinfo }, indent=4)}\n'

@app.route('/kill', methods=['POST'])
def kill() :
	crawlerid = request.json.get('crawler')
	crawlerids = request.json.get('crawlers', [])
	if crawlerid :
		crawlerids.append(crawlerid)

	for crawlerid in crawlerids :
		crawlers[crawlerid].terminate()
		crawlers[crawlerid].join(1)
		crawlersinfo[crawlerid]['uptime'] = time.time() - crawlersinfo[crawlerid]['starttime']

	updateCrawlersInfo()
	return f'{json.dumps({ "killed": crawlerids, "crawlers": crawlersinfo }, indent=4)}\n'

@app.route('/killall', methods=['GET'])
def killall() :
	for key in crawlers.keys() :
		crawlers[key].terminate()
		crawlers[key].join(1)

	updateCrawlersInfo()
	return f'{json.dumps(crawlersinfo, indent=4)}\n'

@app.route('/end', methods=['POST'])
def end() :
	crawlerid = request.json.get('crawler')
	crawlerids = request.json.get('crawlers', [])
	if crawlerid is not None :
		crawlerids.append(crawlerid)

	for crawlerid in crawlerids :
		shutdowns[crawlerid].set()
		crawlersinfo[crawlerid]['status'] = 'gracefully shutting down' if shutdowns[crawlerid].is_set() else 'error, could not set shutdown event'

	updateCrawlersInfo()
	return f'{json.dumps({ "ended": crawlerids, "crawlers": crawlersinfo }, indent=4)}\n'

@app.route('/endall', methods=['GET'])
def endall() :
	for key in crawlers.keys() :
		shutdowns[key].set()
		crawlersinfo[key]['status'] = 'gracefully shutting down' if shutdowns[key].is_set() else 'error, could not set shutdown event'

	updateCrawlersInfo()
	return f'{json.dumps(crawlersinfo, indent=4)}\n'

@app.route('/get', methods=['GET'])
def getinfo() :
	updateCrawlersInfo()
	return f'{json.dumps(crawlersinfo, indent=4)}\n'

@app.route('/get/shutdowns', methods=['GET'])
def getshutdowns() :
	return f'{json.dumps({ k: v.is_set() for k, v in shutdowns.items() }, indent=4)}\n'

@app.route('/clean', methods=['GET'])
def clean() :
	updateCrawlersInfo()
	keys = [key for key in crawlers.keys() if crawlersinfo[key].get('status') == 'dead']
	for key in keys :
		crawler = crawlers.pop(key)
		crawler.join(1)
		crawlersinfo.pop(key)
		shutdowns.pop(key)
	return f'{json.dumps(crawlersinfo, indent=4)}\n'

@app.route('/exit', methods=['GET'])
def eggxit() :
	sys.exit(1)

if __name__ == '__main__' :
	app.run()
