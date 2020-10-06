from pycrawl.common.HTTPError import ResponseNotOk, BadOrMalformedResponse
from pycrawl.common import GetFullyQualifiedClassName, isint
from collections import defaultdict
from lxml.html import fromstring
from traceback import format_tb
import ujson as json
import requests
import logging
import time
import pika
import sys


class First :
	def __init__(self, method=None) :
		self.method = method

	def __call__(self, it) :
		try :
			return next(filter(self.method, it))
		except (TypeError, StopIteration) :
			return None

first = First()


class BaseCrawlingException(Exception) :
	def __init__(self, message, logdata={ }) :
		Exception.__init__(self, message)
		self.logdata = logdata


class WebsiteOffline(BaseCrawlingException) :
	pass


class InvalidResponseType(BaseCrawlingException) :
	pass


class InvalidSubmission(BaseCrawlingException) :
	pass


class NoSubmission(BaseCrawlingException) :
	pass


class ShutdownCrawler(BaseCrawlingException) :
	pass


class Crawler :
	xpathargs = { 'regexp': False, 'smart_strings': False }

	def __init__(self, **kwargs) :
		"""
		startingid: required - first id to crawl
		direction: required - which direction and how much to increment id
		event: python multiprocessing.Event to shutdown crawler gracefully from another process
		backoff: total number of urls to skip before sleeping - positive direction only
		backoffstep: amount to increase backoff by per failure
		skipmaxretries: number of times to retry a url before giving up
		checkevery: interval to check skipped urls in seconds
		timeout: how long to wait when downloading an html document
		idletime: how long to wait when the crawler has caught up with the most recent uploaded submissions
		maxidletime: the longest the bot will wait after repeatedly attemping to crawl new submissions
		calm: force the crawler to act more calmly to reduce strain on the website
		endingid: specify an ending id to stop crawling on
		urls: only crawl these specific urls
		simplelogging: enables terminal based logging (createDefaultLogger)
		"""

		# apply defaults here
		self.id = int(kwargs.get('startingid', 0))
		self.direction = int(kwargs.get('direction', 0))
		self.backoff = self._backoff = int(kwargs.get('backoff', 5))
		self.backoffstep = int(kwargs.get('backoffstep', 5))
		self.skipped = tuple([] for _ in range(kwargs.get('skipmaxretries', 3)))
		self._idlefor = self.idleTime = float(kwargs.get('idletime', 15))
		self.maxIdleTime = float(kwargs.get('maxidletime', 600))
		self.timeout = float(kwargs.get('timeout', 30))
		self.checkEvery = float(kwargs.get('checkevery', 180))
		self.urls = list(set(kwargs.get('urls', [])))


		self.name = f'{self.__class__.__name__}_{self.id}{self.direction:+d}'
		self.calm = bool(kwargs.get('calm', self.direction < 0))
		self.url = None
		self.checkingSkips = None

		self.event = kwargs.get('event')
		if self.event :
			is_set = self.event.is_set
		else :
			is_set = lambda : False

		if 'endingid' in kwargs :
			endingid = int(kwargs.get('endingid'))
			if self.direction > 0 :
				self.done = lambda : self.id > endingid or is_set()
			else :
				self.done = lambda : self.id < endingid or is_set()
		else :
			self.done = is_set

		if kwargs.get('simplelogging') :
			self.createDefaultLogger()
		self.logger = logging.getLogger(self.name)
		self.consecutiveNoSubmissions = 0
		self.sleepfor = None

		self._mq_connection_info = None
		self._mq_publish_info = None
		self._mq_connection = None
		self._mq_channel = None

		# initialize the session
		self._session = requests.Session()

		self.errorHandlers = defaultdict(lambda : self.unexpectedErrorHandler, {  # default, log a critical error
			# all handlers are called WITHOUT args
			# handlers that return True reset skips to 0, if the url needs to be skipped, run self.skipUrl
			BadOrMalformedResponse: self.shutdown,  # FULL SHUTDOWN, KILL PROCESS AND LOG ERROR
			WebsiteOffline: lambda : self.skipUrl(lambda : time.sleep(60 * 60)),  # temporary (60 minute) shutdown
			ResponseNotOk: self.responseNotOkHandler,  # let unique handler deal with it
			InvalidResponseType: self.skipUrl,  # skip, check again later
			requests.exceptions.ConnectionError: lambda : self.skipUrl(lambda : time.sleep(5 * 60)),  # temporary (5 minute) shutdown
			NoSubmission: self.noSubmissionHandler,  # custom handler
			InvalidSubmission: lambda : True,  # a submission was found, but the type isn't able to be indexed
			requests.exceptions.ReadTimeout: self.queueUrl,  # the endpoint is having an issue, re-queue until we get a more solid error
			requests.exceptions.SSLError: self.queueUrl,  # same as above
			ValueError: self.valueErrorHandler,  # custom error handler, make sure it's what we expected
		})

		self.doNotLog = { NoSubmission, ResponseNotOk }  # don't log these errors

		self.unblocking = { }  # empty by default, but is checked based on response code


	def run(self, urls=None) :
		# append the passed urls to the pre-existing internal list of urls
		if isinstance(urls, list) :
			self.urls += urls
		elif isinstance(urls, str) :
			self.urls += urls.split()

		nextcheck = time.time() + self.checkEvery

		try :
			for url in self.urlGenerator() :
				if self.crawl(url) :
					self.consecutiveNoSubmissions = 0
					self._backoff = self.backoff
					self._idlefor = self.idleTime

				if time.time() > nextcheck :
					self.checkSkips()
					nextcheck = time.time() + self.checkEvery

		except :
			self.logger.error({
				'message': f'{self.name} gracefully shutting down.',
				**self.crashInfo(),
			})

		else :
			self.logger.info({
				'message': f'{self.name} gracefully shutting down.',
				'skips': self.skips(),
				'id': self.id,
			})

		if self.event :
			self.event.set()

		# try to gracefully shut down...
		try :
			maxChecks = len(self.skipped)
			while maxChecks > 0 and self.skips() :
				time.sleep(self.checkEvery)
				self.checkSkips()
				maxChecks -= 1

		except :
			logdata = {
				'message': f'{self.name} has shut down.',
				'skipped': self.skipped,
				**self.crashInfo(),
			}
			if self.skips() :
				self.logger.error(logdata)
			else :
				self.logger.info(logdata)

		else :
			logdata = {
				'message': f'{self.name} gracefully finished.',
				'items': self.verboseSkipped(),
				'skips': self.skips(),
				'id': self.id,
			}
			if self.skips() :
				self.logger.error(logdata)
			else :
				self.logger.info(logdata)


	def crashInfo(self) :
		exc_type, exc_obj, exc_tb = sys.exc_info()
		return {
			'error': f'{GetFullyQualifiedClassName(exc_obj)}: {exc_obj}',
			'stacktrace': format_tb(exc_tb),
			'id': self.id,
			'url': self.url,
			'queue': len(self.urls),
			'formattedurl': self.formattedurl,
			'name': self.name,
			'skips': self.totalSkipped(),
			**getattr(exc_obj, 'logdata', { }),
		}


	def checkSkips(self) :
		startingSkips = self.skips()
		self.checkingSkips = True

		maxlen = len(self.skipped) - 1
		for i in range(maxlen, -1, -1) :
			while self.skipped[i] :
				url = self.skipped[i].pop()
				if self.crawl(url) :
					pass  # use pass rather than not because it's easier to read
				elif i < maxlen :
					self.skipped[i+1].append(url)

		self.checkingSkips = False

		self.logger.info({
			'message': f'{self.name} checked skips.',
			'startingskips': startingSkips,
			'skips': self.skips(),
			'id': self.id,
		})


	def skipUrl(self, func=None) :
		if not self.checkingSkips :
			# don't add if checking skips, let skip logic handle that
			self.skipped[0].append(self.url)
		if func :
			return func()


	def queueUrl(self, url=None) :
		# different from skip, this function has no limit to number of tries the url can be re-crawled
		self.urls.append(url or self.url)


	def skips(self) :
		return self.totalSkipped()


	def totalSkipped(self) :
		return sum(map(len, self.skipped))


	def prettySkipped(self) :
		return tuple(map(len, self.skipped))


	def verboseSkipped(self) :
		return str(self.skipped)


	def idle(self) :
		startime = time.time()
		self.checkSkips()
		endtime = time.time()

		# sleep off remainder of the time left
		remainder = startime - endtime + self._idlefor
		if remainder > 0 :
			time.sleep(remainder)

		self._idlefor = min(self._idlefor * 2, self.maxIdleTime)


	def formatUrl(self, url) :
		return url


	def crawl(self, url) :
		# returns True if the crawl was successful or otherwise shouldn't be run again
		self.url = url
		self.formattedurl = self.formatUrl(url)
		self.sleepfor = 0
		try :
			result = self.parse(self.downloadHtml(self.formattedurl))
			self.postProcessing(result)

			self.send(result)

			if self.sleepfor and self.calm :
				time.sleep(self.sleepfor)

			return True

		except :
			typeE = sys.exc_info()[0]
			if typeE not in self.doNotLog and typeE in self.errorHandlers :
				self.logger.info(self.crashInfo())
			return self.errorHandlers[typeE]()


	def postProcessing(self, result) :
		pass


	def unexpectedErrorHandler(self) :
		self.logger.critical({
			'message': 'encountered unexpected error.',
			**self.crashInfo(),
		})


	def shutdown(self) :
		self.logger.error(self.crashInfo())
		e, exc_tb = sys.exc_info()[1:]
		raise ShutdownCrawler(e).with_traceback(exc_tb)


	def responseNotOkHandler(self) :
		e = sys.exc_info()[1]
		hundredCode = int(e.status / 100)
		if hundredCode == 0 :  # custom error, no response received
			self.queueUrl()
			self.logger.warning({
				**self.crashInfo(),
				'message': f'{self.name} encountered {e}.',
				'error': f'{e.status} {GetFullyQualifiedClassName(e)}: {e}',
			})
		elif hundredCode == 4 :  # 400 error
			self.skipUrl()
			self.logger.warning({
				**self.crashInfo(),
				'message': f'{self.name} encountered {e}.',
				'error': f'{e.status} {GetFullyQualifiedClassName(e)}: {e}',
			})
		elif hundredCode == 5 :  # 500 error
			self.queueUrl()
			time.sleep(5 * 60)  # sleep for a while
		else :
			self.queueUrl()
			self.logger.error({
				**self.crashInfo(),
				'message': f'{self.name} caught unexpected error.',
				'error': f'{e.status} {GetFullyQualifiedClassName(e)}: {e}',
			})


	def noSubmissionHandler(self) :
		if self.direction > 0 and not self.checkingSkips :
			self.skipUrl()
			self.consecutiveNoSubmissions += 1
			if self.consecutiveNoSubmissions >= self._backoff :
				self._backoff += self.backoffstep
				startingSkips = self.totalSkipped()
				del self.skipped[0][-self.consecutiveNoSubmissions:]  # remove skips
				self.id -= self.consecutiveNoSubmissions * self.direction
				self.logger.info({
					'message': f'{self.name} encountered {self.consecutiveNoSubmissions} urls without submissions, sleeping for {self._idlefor}s.',
					'startingskips': startingSkips,
					'skips': self.skips(),
					'id': self.id,
				})
				self.consecutiveNoSubmissions = 0  # and reset to zero
				self.idle()  # chill to let more stuff be uploaded

			# don't reset skips, this function handles that
			return False

		else :
			# return true, we don't want to revisit this url
			return True


	def valueErrorHandler(self) :
		e = sys.exc_info()[1]
		if str(e).startswith('Unicode strings with encoding declaration are not supported.') :
			self.queueUrl()
		else :
			# we can't just re-raise it because we've exited the crawl try block
			self.unexpectedErrorHandler()


	def urlGenerator(self) :
		if self.urls :
			# only run on the urls provided
			while self.urls :
				yield self.urls.pop(0)
			return

		# now create a generator
		while not self.done() :
			yield self.id
			self.id += self.direction
			# in case we need to re-queue a url for some reason
			for _ in range(len(self.urls)) :
				yield self.urls.pop(0)


	def downloadHtml(self, url) :
		try :
			response = self._session.get(url, timeout=self.timeout)
		except :
			raise ResponseNotOk(f'timeout ({self.timeout}) elapsed for url: {url}', status=-1)
		else :
			if response.ok : return fromstring(response.text)
			elif response.status_code in self.unblocking :
				self.unblocking[response.status_code](response)
			raise ResponseNotOk(f'reason: {response.reason}, url: {url}', status=response.status_code, logdata={ 'reason': response.reason })
		raise InvalidResponseType(f'request failed for an unknown reason.')


	def send(self, item) :
		self.logger.info(f'[{time.asctime(time.localtime(time.time()))}] crawled > {json.dumps(item, indent=4)}')


	def _send(self, message) :
		# message should be a bytestring
		if not isinstance(message, bytes) :
			raise ValueError('message must be of type bytes.')
		for _ in range(3) :
			try :
				self._mq_channel.basic_publish(**self._mq_publish_info, body=message)
				return True
			except (pika.exceptions.ConnectionWrongStateError, pika.exceptions.StreamLostError, pika.exceptions.ChannelWrongStateError) :
				# reconnect
				self._mq_connect()
		self.logger.critical(f'{self.name} failed to send item to message queue.')
		return False


	def mqConnect(self, connection_info=None, exchange_info=None, publish_info=None) :
		self._mq_connection_info = connection_info
		self._mq_exchange_info = exchange_info
		self._mq_publish_info = publish_info
		try :
			self._mq_connect()
		except Exception as e :
			self.logger.warning(
				f'{self.name} encountered '
				f'{GetFullyQualifiedClassName(e)}' if str(e) else f'{GetFullyQualifiedClassName(e)}: {e}'
				' and cannot write to message queue. Messages will write to terminal.'
			)
		else :
			self.send = lambda x : self._send(json.dumps(x).encode())


	def _mq_connect(self) :
		self._mq_connection = pika.BlockingConnection(pika.ConnectionParameters(**self._mq_connection_info))
		self._mq_channel = self._mq_connection.channel()
		if self._mq_exchange_info :
			self._mq_channel.exchange_declare(**self._mq_exchange_info)
		else :
			self._mq_channel.queue_declare(queue=self._mq_publish_info['routing_key'])


	def createDefaultLogger(self) :
		logging.basicConfig(level=logging.INFO)
