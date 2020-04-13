from pycrawl.crawler import Crawler, first

class SimpleCrawler(Crawler) :

	def parse(self, document) :

		# tell the crawler to sleep for 5 seconds after this crawl if in calm mode
		self.sleepfor = 5

		# run some xpaths to grab your content, using first() to grab the first node with content
		title = first(document.xpath('//head/title/text()', **self.xpathargs))

		# you may want to include **self.xpathargs to make the path run faster
		description = first(document.xpath('//head/meta[@name="description"]/@content', **self.xpathargs))

		# return your item to be passed to the message queue for later consumption
		return {
			'title': title,
			'description': description,
			'id': self.id,
			'url': self.url,
			'calm': self.calm,
		}


"""
# to run:

from pycrawl.examples.crawlers import SimpleCrawler
a = SimpleCrawler(startingid=0, direction=0, simplelogging=True)
a.run(['https://www.google.com/'])
"""
