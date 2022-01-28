import os
import boto3

from .dynamodb import DynamoDB
from .s3 import S3

class S3DynamoDB(DynamoDB):
  def __init__(self,**kwargs):
    super().__init__(**kwargs)
    self.s3=S3(**kwargs)
  
  def download(self,id,**kwargs):
    return self.s3.download(id,**kwargs) 
  
  def upload(self,filepath,id=None,overwrite=False,**kwargs):
    id = self.s3.upload(filepath,id,overwrite=overwrite,**kwargs)
    ext = filepath.split(os.extsep).pop()
    self.insert(id,ext=ext,**kwargs)
    return id

  def delete(self,id):
    self.s3.delete(id)
    super().delete(id)
    
  def uploadFolder(self,folder=None,**kwargs):
    if folder is None:folder=self.s3.CacheLocation
    
    files=[]
    for r, d, f in os.walk(folder):
      for file in f:
        files.append(os.path.join(r,file))
    
    for filepath in files:
      Key=self.s3._getKey(filepath,folder)
      self.upload(filepath,id,**kwargs)
      
    
    return True    