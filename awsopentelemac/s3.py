import os
import stat
import shutil
import boto3
from tqdm.auto import tqdm
from botocore.errorfactory import ClientError
from boto3.s3.transfer import TransferConfig
import threading
from .utils import join

GB = 1024 ** 3

class ProgressPercentage(object):
  def __init__(self, t):
    self.t = t
    self._lock = threading.Lock()
  def __call__(self, bytes_amount):
    with self._lock:
      self.t.update(bytes_amount)


class S3(object):
  def __init__(self,**kwargs):
    self.BucketName    = kwargs.get('BucketName',os.environ.get('AWS_BUCKETNAME',None))
    self.ProfileName   = kwargs.get('ProfileName',os.environ.get('AWS_PROFILENAME',None))
    self.CacheLocation = kwargs.get('CacheLocation',os.environ.get('AWS_CACHELOCATION',None))
    self.S3Prefix      = kwargs.get('S3Prefix',os.environ.get('AWS_S3PREFIX',""))
    
    if self.BucketName is None:raise Exception("BucketName or AWS_BUCKETNAME was not set")
    if self.CacheLocation is None:raise Exception("CacheLocation or AWS_CACHELOCATION was not set")
    
    os.makedirs(self.CacheLocation, exist_ok=True)
    
    session = boto3.Session(profile_name=self.ProfileName)
    self.s3 = session.client('s3')
    
    self.transferconfig = TransferConfig(
      multipart_threshold=int(5*GB),
      multipart_chunksize=int(1*GB),
      max_concurrency=int(os.environ.get('MAX_CONCURRENCY',"10"))
      )



  
  def _getCachePath(self,Key):
    path = os.path.relpath(Key,self.S3Prefix)
    return join(self.CacheLocation,path)

  def getKeys(self,folder=None):
    Prefix=join(self.S3Prefix,folder) if folder else self.S3Prefix
    page_iterator = self.s3.get_paginator('list_objects_v2').paginate(Bucket=self.BucketName, Prefix=Prefix)
    files=[]
    for page in page_iterator:
      if "Contents" in page:
        files=files+page["Contents"]
    return files

  def deleteAllS3(self):
    files = self.getKeys()
    if len(files)==0:return
    keys = [{"Key":file["Key"]} for file in files]
    self.s3.delete_objects(Bucket=self.BucketName, Delete={"Objects":keys})


  def deleteAllCache(self,Prefix=None):
    path = self.CacheLocation
    if Prefix:path=join(self.CacheLocation,Prefix)
    shutil.rmtree(path, ignore_errors=True, onerror=None)
    if not Prefix:os.mkdir(path)
  
  def delete(self,Key):
    self.deleteS3(Key)
    self.deleteCache(Key)
  
  def deleteS3(self,Key):
    if self.exists(Key):
      response=self.s3.delete_objects(Bucket=self.BucketName, Delete={"Objects":[{"Key":Key}]})
  
  
  def deleteCache(self,Key):
    filepath=self._getCachePath(Key)
    if os.path.exists(filepath):
      os.remove(filepath)


  def exists(self,Key):
    try:
      self.s3.head_object(Bucket=self.BucketName, Key=Key)
      return True
    except ClientError as e:
      return False
  
  
  def download(self,Key,progress=True,overwrite=False):
    filepath=self._getCachePath(Key)
    folder = os.path.dirname(filepath)
    if not os.path.exists(folder): os.makedirs(folder,exist_ok=True)
    if progress:
      file_object = self.s3.get_object(Bucket=self.BucketName, Key=Key)
      filesize    = file_object.get("ContentLength")
      with tqdm(total=int(filesize), unit='B', unit_scale=True) as t:
        with open(filepath, 'wb') as data:
          self.s3.download_fileobj(self.BucketName,Key,data,Config=self.transferconfig,Callback=ProgressPercentage(t))
    else:
      self.s3.download_file(self.BucketName, Key, filepath,Config=self.transferconfig)
    return filepath
  
  
  def downloadFolder(self,folder=None,**kwargs):
    files=self.getKeys(folder)
    for file in files:
      Key=file['Key']
      self.download(Key,**kwargs)
  
    
  def upload(self,filepath,Key=None,progress=True,ExtraArgs={},overwrite=True,**kwargs):
    if not os.path.exists(filepath):raise Exception("File({}) does not exist".format(filepath))
    
    key = os.path.basename(filepath) if Key is None else Key
    Key=  join(self.S3Prefix,key)
    
    if self.exists(Key) and not overwrite:return key
    
    if progress:
      filesize=os.path.getsize(filepath)
      with tqdm(total=filesize, unit='B', unit_scale=True, position=0, leave=True) as t:
        with open(filepath, 'rb') as data:
          self.s3.upload_fileobj(data, self.BucketName, Key,Config=self.transferconfig,Callback=ProgressPercentage(t))
    else:
      self.s3.upload_file(filepath, self.BucketName,Key,Config=self.transferconfig)
    return key


  def _getKey(self,filepath,folder):
    path = os.path.relpath(filepath,folder)
    return join(self.S3Prefix,path)


  def uploadFolder(self,folder=None,overwrite=False,**kwargs):
    if folder is None:folder=self.CacheLocation
    
    files=[]
    for r, d, f in os.walk(folder):
      for file in f:
        files.append(join(r,file))
    
    for filepath in files:
      path = os.path.relpath(filepath,folder)
      Key  = join(self.S3Prefix,path)
      if not self.exists(Key) or overwrite:
        self.upload(filepath,Key,**kwargs)