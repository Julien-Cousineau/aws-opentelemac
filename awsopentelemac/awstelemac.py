import os
import shutil
import subprocess

from .batch import Batch
from .s3dynamodb import S3DynamoDB

from slfpy import SLF
from .telemac.telemac_cas import TelemacCas
from .utils import join

class AWSOpenTelemac(Batch):
  def __init__(self,**kwargs):
    TableCas    = kwargs.pop('TableCas',os.environ.get('AWS_TABLECAS',None))
    TableData   = kwargs.pop('TableData',os.environ.get('AWS_TABLEDATA',None))
    super().__init__(TableName=TableCas,**kwargs)
    self.data=S3DynamoDB(TableName=TableData,**kwargs)
    
  @property
  def CacheLocation(self):
    return self.data.s3.CacheLocation
  
  
  def getRel(self,id,filepath):
      return os.path.relpath(filepath,join(self.CacheLocation,os.path.dirname(id)))
    
    
  def addCas(self,id,module="telemac2d",keywords={}):
    if not isinstance(keywords,dict):raise Exception("Needs a dict")
    study=TelemacCas(module)
    study.setValues(keywords)
    item=self.insert(id=id,module=module,keywords=study.values)
    return item
    
    
  def update(self,id,keywords,**kwargs):
    item=self.get(id)
    if not item:raise Exception("Cas does not exist - {}".format(id))
    if not isinstance(keywords,dict):raise Exception("Needs a dict")
    study=TelemacCas(item['module'])
    study.setValues(keywords)
    return super().update(id,keywords=study.values)
  
  def _inputPath(self,id,folder,filepath):
    root=os.path.dirname(filepath)
    name=os.path.basename(filepath)
    return join(os.path.dirname(filepath),folder,os.path.basename(filepath))
  
  def updateProgress(self,id,iframe,nframe):
    super().update(id,iframe=iframe,nframe=nframe)
  
  def download(self,id):
    return self.data.download(id)
  
  def uploadFile(self,filepath,root="",overwrite=False):
    return self.data.upload(filepath,join(root,"input",os.path.basename(filepath)),overwrite=overwrite)

  def uploadFortran(self,filepath,root="",overwrite=False):
    return self.data.upload(filepath,join(root,"user_fortran",os.path.basename(filepath)),overwrite=overwrite)
  
  def uploadFromCas(self,id,casPath,module="telemac2d",overwrite=False,keywords={},**kwargs):
    study=TelemacCas(module,casPath)
    for key in study.in_files:
      if key=="FORTRAN FILE":
        study.values[key]=[self.data.upload(file,join(os.path.dirname(id),"user_fortran",os.path.basename(file)),overwrite=overwrite) for file in study.values[key]]
      else:
        filepath=study.values[key]
        study.values[key]=self.data.upload(filepath,join(os.path.dirname(id),"input",os.path.basename(filepath)),overwrite=overwrite)
    
    # Remove Windows "\\" to /
    for key in study.values:
      v=study.values[key]
      if isinstance(v,str):study.values[key]=v.replace(r"\\","/")
    
    return self.insert(id=id,module=module,keywords={**study.values,**keywords})
    
  
  def addFortranToCas(self,id,Key):
    item=self.get(id)
    if not item:raise Exception("Cas does not exist - {}".format(id))
    farray=item['keywords'].get("FORTRAN FILE",[])
    farray.append(Key)
    return self.update(id,{"FORTRAN FILE":farray})

  
  def rmFortranFromCas(self,id,fortranId,**kwargs):
    item=self.get(id)
    if not item:raise Exception("Cas does not exist - {}".format(id))
    farray=item['keywords'].get("FORTRAN FILE",[])
    farray.remove(fortranId)
    item=self.update(id,{"FORTRAN FILE":farray})
    self.data.delete(id)
  
  def AWS(self,id,cas=None,download=True,**kwargs):
    if cas:self.uploadFromCas(id,cas,**kwargs)
      
    _,status = super().AWS(id,**kwargs)
    item=self.get(id)
    if download and status=="SUCCEEDED":
      output=item.get("output",None)
      if output:return self.download(output)
    return item

  def run(self,id,api=False):
    super().update(id,status="RUNNING")
    try:
      item     = self.get(id)
      module   = item.get("module")
      keywords = item.get("keywords")
      
      study=TelemacCas(module)
      study.setValues(keywords)
      
      # Download files
      for key in study.in_files:
        value=study.values[key]
        if isinstance(value,list):
          # FORTRAN FILES
          study.values[key]=[self.getRel(id,self.data.download(_value)) for _value in value]
        else:
          study.values[key]=self.getRel(id,self.data.download(value))
      
      casPath = join(self.CacheLocation,os.path.dirname(id),"temp.cas")
      
      study.write(casPath)
      
      # Run
      CPUs=os.environ.get("AWS_CPU",1)
      currentDirectory=os.getcwd()
      if api:
        shutil.copy2('runapi.py', join(self.CacheLocation,os.path.dirname(id),"runapi.py")) 
        os.chdir(os.path.dirname(casPath))
        print('mpirun --allow-run-as-root -n {} python runapi.py {} {}'.format(CPUs,id,module))
        subprocess.call('mpirun --allow-run-as-root -n {} python runapi.py {} {}'.format(CPUs,id,module),shell=True)
      else:
        os.chdir(os.path.dirname(casPath))
        subprocess.call('{}.py --ncsize={} {}'.format(module,CPUs,casPath),shell=True)
      os.chdir(currentDirectory)
      
      outputPath = join(self.CacheLocation,os.path.dirname(id),keywords['RESULTS FILE'])
      
      output = join(os.path.dirname(id),"output","{}.slf".format(os.path.basename(id)))
      self.data.upload(outputPath,output,overwrite=True)
      super().update(id,status="SUCCEEDED",output=output)
      return outputPath
    except Exception as error:
      print(error)
      super().update(id,status="FAILED",error=repr(error))
    