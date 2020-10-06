from setuptools import setup, find_packages

setup(
	name='pycrawl',
	version='0.1.2',
	description='A fast and simple crawling framework.',
	author='kheina',
	url='https://github.com/kheina/pycrawl',
	packages=find_packages(),
	install_requires=[
		'flask',
		'lxml',
		'requests',
		'pika',
		'ujson',
	],
)