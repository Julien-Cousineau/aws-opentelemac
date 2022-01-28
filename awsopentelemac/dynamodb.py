import json
import os
import time

import decimal
import numpy as np
import boto3
from boto3.dynamodb.conditions import Key, Attr,ConditionBase
import operator
import copy

ops = { 
  r"&": operator.and_,
  r"|": operator.or_,
  "!": operator.invert,
  "+": operator.add, 
  "-": operator.sub,
}

class DecimalEncoder(json.JSONEncoder):
  """ DecimalEncoder to transform DynamoDB object to Python Object
  """
  def default(self, obj):
    if isinstance(obj, decimal.Decimal):
      n=float(obj)
      if n.is_integer():n=int(n)
      return n
    elif isinstance(obj, np.integer):
        return int(obj)
    elif isinstance(obj, np.floating):
        return float(obj)
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    

    return super(DecimalEncoder, self).default(obj)

def clean_empty(d):
  """ Remove keys with None or empty list throughout dictionary
  """
  if not isinstance(d, (dict, list)):
    return d
  if isinstance(d, list):
    return [v for v in (clean_empty(v) for v in d) if v]
  return {k: v for k, v in ((k, clean_empty(v)) for k, v in d.items()) if v}

def _formatD2P(obj):
  """ Format DynamoDB object to python object
  """
  return json.loads(json.dumps(obj,cls=DecimalEncoder))

def _formatP2D(obj):
  """ Format python object to DynamoDB object
  """
  return json.loads(json.dumps(obj,cls=DecimalEncoder), parse_float=decimal.Decimal)

def _build(fe):
  """ Build filter expression from dict
  """
  if not fe.get("con"):raise Exception("FilterExpression needs a con")
  if fe.get("value",None) is None:raise Exception("FilterExpression needs a value")
  con=fe.get("con")
  value=fe.get("value")
  if con=="between" and not isinstance(value,tuple):raise Exception("Between condition needs a tuple")
  if fe.get("Key"):return getattr(Key(fe.get("Key")),con)(value)
  if fe.get("Attr"):
    if con=="exists":return getattr(Attr(fe.get("Attr")),con)()
    return getattr(Attr(fe.get("Attr")),con)(value)
  raise Exception("FilterExpression needs Key or Attr")

def getFilterExpression(fe):
  """ Build filter expression from list,dict or ConditionBase
  """
  if isinstance(fe,ConditionBase):
    pass
  elif isinstance(fe,dict):
    fe=_build(fe)
  elif isinstance(fe,list) and len(fe)>0:
    new=_build(fe[0])
    for i in range(1,len(fe)):
      _=fe[i]
      op=ops.get(_.get('ops',"&"),None)
      if op is None:raise Exception("Operator does not exist")
      new = op(new,_build(_))
    fe=new
  else:
    raise Exception("FilterExpression needs to be ConditionBase,dict or list")
  return fe


class DynamoDB(object):
  """
  Parameters
  ----------
  TableName:str
    Name of DynamoDB Table
  """
  def __init__(self,**kwargs):
    
    self.TableName    = kwargs.pop('TableName',os.environ.get('AWS_TABLENAME',None))
    self.ProfileName  = kwargs.get('ProfileName',os.environ.get('AWS_PROFILENAME',None))
    if self.TableName is None:raise Exception("TableName is not set")
    
    
    session=boto3.Session(profile_name=self.ProfileName)
    dynamodb = session.resource('dynamodb')
    self.table=dynamodb.Table(self.TableName)
    

  
  def insert(self,id,**kwargs):
    if self.exists(id):return self.update(id,**kwargs)  
    
    timestamp = int(time.time()*1000)
    item = {
      'id': id,
      'createdAt': timestamp,
      'updatedAt': timestamp,
      **kwargs
    }
    return _formatD2P(self.table.put_item(Item=_formatP2D(item)))

  def delete(self,id):
    return self.table.delete_item(Key={'id': id})
  
  def get(self,id):
    response = self.table.get_item(Key={'id': id})
    return _formatD2P(response.get("Item",{}))
  
  def exists(self,id):
    item=self.get(id)
    return bool(item)
  
  def update(self,id,**kwargs):
    kwargs = _formatP2D(kwargs)
  
    new={}
    exp=["#updatedAt=:updatedAt"]
    ExpressionAttributeNames={"#updatedAt":"updatedAt"}
    for key in kwargs:
      ExpressionAttributeNames['#'+key]=key
      if isinstance(kwargs[key],dict):
        """ Update object. If the attribute is object, it needs to download 
            the object from DynamoDB and updates it properties.
        """
        response = self.table.get_item(Key={'id': id})
        item=response.get("Item",{})
        if key in item and isinstance(item[key],dict):
          kwargs[key]={**item[key],**kwargs[key]}
          # kwargs[key]=clean_empty({**item[key],**kwargs[key]})
        
      new[":"+key]=kwargs[key]
      exp.append("#{0}=:{0}".format(key))
    exp=",".join(exp)
    
    timestamp = int(time.time()*1000)
    ExpressionAttributeValues={':updatedAt': timestamp,**new}
    
    response = self.table.update_item(
        Key={'id': id},
        ExpressionAttributeNames=ExpressionAttributeNames,
        ExpressionAttributeValues=ExpressionAttributeValues,
        UpdateExpression='SET {}'.format(exp),
        ReturnValues='ALL_NEW',
        
    )
    
    return _formatD2P(response.get('Attributes',{}))

  
  def all(self):
    response = self.table.scan()
    return _formatD2P(response.get("Items",[]))
  
  def query(self,KeyConditionExpression=None,**kwargs):
    kwargs = _formatP2D(kwargs)
    """ 
    Query/Scan DynamoDB
    
    Parameters
    ----------
    KeyConditionExpression:dict or list of dict
    FilterExpression:dict or list of dict
      dict:
        ops:str
        Key:str,required/optional
        Attr:str,optional/required
        con:Key operator (begins_with,between,eq,gt,gte,lt,lte)
        con:Attr operator (attribute_type,begins_with,between,contains,exists,is_in,ne,not_exists,size,eq,gt,gte,lt,lte)
        value:*
    ProjectionExpression:str,
    ExpressionAttributeNames=dict,
    IndexName:str
    
    Examples
    --------
    FilterExpression=[{"Key":"year","con":"between","value":(1950,1959)}]
    ProjectionExpression="#yr, title, info.rating"
    ExpressionAttributeNames= { "#yr": "year", }
    """
    
    if KeyConditionExpression is None:raise Exception("Query needs KeyConditionExpression")
    kwargs["KeyConditionExpression"]=getFilterExpression(KeyConditionExpression)
    return self._queryscan("query",**kwargs)

  
  def scan(self,**kwargs):
    kwargs = _formatP2D(kwargs)
    return self._queryscan("scan",**kwargs)
    
  scan.__doc__=query.__doc__
  
  def _queryscan(self,action='query',**kwargs):
    kwargs = _formatP2D(kwargs)
    """ Query and scan general functions
    """
    if kwargs.get("FilterExpression"):
      kwargs["FilterExpression"]=getFilterExpression(kwargs.get("FilterExpression"))
    
    response = getattr(self.table,action)(**kwargs)
    data=response.get("Items",[])
    while response.get('LastEvaluatedKey', False):
      response = getattr(self.table,action)(**kwargs,ExclusiveStartKey=response['LastEvaluatedKey'])
      data.extend(response['Items'])
    return _formatD2P(data)