def GetFullyQualifiedClassName(obj) :
	module = getattr(obj, '__module__', None)
	if module and module != str.__module__ :
		return f'{module}.{obj.__class__.__name__}'
	return obj.__class__.__name__


def isint(s) :
	try : return int(s)
	except : return None
