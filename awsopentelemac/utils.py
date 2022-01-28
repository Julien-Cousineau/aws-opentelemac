import os
import json
from functools import wraps

def loadJSON(filepath):
  """ Loading json file
  """
  with open(filepath, 'r') as f:
    return json.load(f)

def setENV(path):
    obj=loadJSON(path)
    for key in obj:
        os.environ[key] = obj[key]
    return obj

def join(*args):
  return os.path.join(*args).replace("\\","/")
    
def checkId(func):
  @wraps(func)
  def wrapper(self,*args,**kwargs):
    if len(args)==0 and not "id" in kwargs:raise Exception("id needs to be set")
    
    if not isinstance(kwargs['id'],str):raise Exception("id needs to be a string")
    return func(self,*args,**kwargs)
  return wrapper    