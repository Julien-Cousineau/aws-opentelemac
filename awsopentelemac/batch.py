from __future__ import unicode_literals
import os
import subprocess
import boto3
import time



from .dynamodb import DynamoDB


def dict2array(d):
  array=[]
  for key in d:
    array.append({'name': key,'value': d[key]})
  return array

class Batch(DynamoDB):
  """
  Parameters
  ----------
  
  Examples
  --------
  
  """
  def __init__(self,environment={},environmentDocker={},**kwargs):
    super().__init__(**kwargs)
    self.jobQueue           = kwargs.get('jobQueue',os.environ.get('AWS_JOBQUEUE',None))
    self.jobDefinition      = kwargs.get('jobDefinition',os.environ.get('AWS_JOBDEFINITION',None))
    if self.jobQueue is None:raise Exception("jobQueue or AWS_JOBQUEUE was not set")
    if self.jobDefinition is None:raise Exception("jobDefinition or AWS_JOBDEFINITION was not set")
    
    self.ProfileName        = kwargs.get('ProfileName',os.environ.get('AWS_PROFILENAME',None))
    session                 = boto3.Session(profile_name=self.ProfileName)
    self.batch              = session.client('batch')
    self.environment        = environment
    self.environmentDocker  = environmentDocker
    
  
  def getStatus(self,id):
    item  = self.get(id)
    jobId = item.get("jobId",None)
    jobs  = self.batch.describe_jobs([jobId])
    if len(jobs)==0:return None
    return jobs[0]['status']  
  
  def wait(self,id):
    item  = self.get(id)
    jobId = item.get("jobId",None)
    
    from halo import Halo
    spinner = Halo(text='SUBMITTED', spinner='dots')
    spinner.start()
    status=""
    while True:
      time.sleep(2.0)
      jobs  = self.batch.describe_jobs(jobs=[jobId])['jobs']
      
      if len(jobs)==0:
        spinner.fail("JobID does not exist")
        break
      
      status=jobs[0]['status']
      
      if status=="SUCCEEDED":
        spinner.succeed(status)
        break
      if status=="FAILED":
        spinner.fail(status)
        break
      if status=="RUNNING":
        spinner.info("RUNNING")
        break
      spinner.text=status
    spinner.stop()
    
    if status=="RUNNING":
      from tqdm import tqdm
      pbar = None
      while True:
        time.sleep(2.0)
        item   = self.get(id)
        jobs  = self.batch.describe_jobs(jobs=[jobId])['jobs']
        
        if len(jobs)==0:
          spinner.fail("JobID does not exist")
          break
        
        status=jobs[0]['status']
        
        if status=="SUCCEEDED":
          if pbar:pbar.close()
          spinner.succeed(status)
          break
        
        if status=="FAILED":
          if pbar:pbar.close()
          spinner.fail(status)
          break
        
        nframe = item.get("nframe",None)
        if nframe is None:continue
        if pbar is None:pbar=tqdm(total=nframe)
        pbar.n = item['iframe']
        pbar.refresh()
      
    super().update(id,status=status)
    return status
  
  def AWS(self,id,vcpus=None,memory=None,wait=True,**kwargs):
    jobQueue      = self.jobQueue
    jobDefinition = self.jobDefinition
    dependsOn     = kwargs.get("dependsOn",[])
    
    environment = {**self.environment,"AWS_CASID":id,"AWS_CACHELOCATION":"/data","AWS_DEFAULT_REGION":"us-east-1"}
    environment.pop("AWS_PROFILENAME",None)
  
    resourceRequirements=[]
    if vcpus:
      environment["AWS_CPU"]="{}".format(vcpus)
      resourceRequirements.append({"type":"VCPU","value":"{}".format(vcpus)})
      if memory is None:memory=int(vcpus*2000*0.95)
      
    if memory:
      environment["AWS_MEMORY"]="{}".format(memory)
      resourceRequirements.append({"type":"MEMORY","value":"{}".format(memory)})
    
    containerOverrides={
      'environment': dict2array(environment),
      "resourceRequirements":resourceRequirements
    }
    
    response=self.batch.submit_job(
      jobName=id.replace("/","-"),
      jobQueue=jobQueue,
      jobDefinition=jobDefinition,
      dependsOn=dependsOn,
      containerOverrides=containerOverrides
    )
    jobId=response['jobId']
    item=super().update(id,jobId=jobId,status="CREATED")
    
    if wait:status=self.wait(id)
    
    return item,status
    
  
  def Docker(self,id,vcpus=None):
    environment = {**self.environment,"AWS_CASID":id,"AWS_CACHELOCATION":"/data"}
    environment.pop("AWS_PROFILENAME",None)
    if vcpus:
      environment["AWS_CPU"]="{}".format(vcpus)
    
    _environ=[]
    for key in environment:
      _environ.append('-e')
      _environ.append('{}={}'.format(key,environment[key]))    
    
    environmentDocker=self.environmentDocker
    OS_CREDENTIALS  = environmentDocker['OS_CREDENTIALS']
    LOCALPARENTPATH = environmentDocker['LOCALPARENTPATH']
    AWS_REPO        = environmentDocker['AWS_REPO']
    OS_NAME         = environmentDocker.get('OS_NAME','root')
    
    OS_DATA         = "{}/data".format(LOCALPARENTPATH)
    DOCKER_DATA     = "/data"
    
    subprocess.run(["docker", "run",
      "-v",r'{}:{}'.format(OS_DATA,DOCKER_DATA),
      "-v",r'{}:/{}/.aws/credentials:ro'.format(OS_CREDENTIALS,OS_NAME),
      *_environ,
      "--interactive",
      "--tty",
      AWS_REPO
      ])