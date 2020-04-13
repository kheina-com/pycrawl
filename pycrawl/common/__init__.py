def GetFullyQualifiedClassName(obj) :
	module = getattr(obj, '__module__', None)
	if module and module != str.__module__ :
		return f'{module}.{obj.__class__.__name__}'
	return obj.__class__.__name__


def setupLogging() :
	try :
		from google.auth import compute_engine
		from google.cloud import logging
		import logging
		credentials = compute_engine.Credentials()
		logging_client = logging.Client(credentials=credentials)
		client.setup_logging()
	except :
		pass


def isint(s) :
	try : return int(s)
	except : return None
