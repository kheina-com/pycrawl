from setuptools import setup

setup(
	name='pycrawl',
	version='0.1',
	description='A fast and simple crawling framework.',
	author='kheina',
	url='https://github.com/kheina/pycrawl',
	packages=['pycrawl'],
	install_requires=[
		'flask',
		'lxml',
		'requests',
		'pika',
		'ujson',
	],
)