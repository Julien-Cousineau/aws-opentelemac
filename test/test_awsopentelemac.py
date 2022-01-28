from __future__ import unicode_literals
# import pytest
import os
import numpy as np
from copy import deepcopy as dc
from awsopentelemac import AWSOpenTelemac
from slfpy import SLF
import subprocess


def setEnv(obj):
  for key in obj:
    os.environ[key]=obj[key]

environment={
  "AWS_PROFILENAME":"tara2",
  "AWS_BUCKETNAME":"tara2",
  "AWS_TABLECAS":"tara2-cas",
  "AWS_TABLEDATA":"tara2",
  "AWS_CACHELOCATION":"/home/ec2-user/environment/cccris/ebs/data/awsopentelemactest",
  "AWS_S3PREFIX":"",
  "AWS_CPU":"2",
  "AWS_JOBQUEUE":"c6gd",
  "AWS_JOBDEFINITION":"tara2",
}

environmentDocker={
  "AWS_REPO":"awsopentelemac",
  "OS_CREDENTIALS":"/home/ec2-user/.aws/credentials_jcousineau",
  "LOCALPARENTPATH":'/home/ec2-user/environment/cccris/ebs',
  "OS_NAME":"root",
}

setEnv(environment)

api=AWSOpenTelemac(environment=environment,environmentDocker=environmentDocker)


def test_AWSOpenTelemac():
  id="confluence/t2d_confluence"
  api.uploadFromCas(id,'test/data/telemac2d/confluence/t2d_confluence.cas',overwrite=True)
  api.update(id,{"FRICTION COEFFICIENT":30})
  lqd=api.uploadFile('test/data/telemac2d/confluence/dummy.lqd')
  api.update(id,{"LIQUID BOUNDARIES FILE":"input/dummy.lqd"})
  api.uploadFortran('test/data/telemac2d/confluence/other.f')
  api.addFortranToCas(id,"user_fortran/other.f")


def test_ApiTelemac_Run():
  cwd = os.getcwd()
  id="confluencerun/t2d_confluencev1"
  api.uploadFromCas(id,'test/data/telemac2d/confluence/t2d_confluence.cas')
  output=api.run(id,api=True)
  
  os.chdir(cwd)
  slf_original=SLF('test/data/telemac2d/confluence/f2d_confluence.slf')
  slf_simulated=SLF(output)
  np.testing.assert_array_almost_equal(slf_original.values, slf_simulated.values,decimal=3)


def test_ApiTelemac_RunParallel():
  pass
  # cwd = os.getcwd()
  # id="test/malpasset"
  # api.uploadFromCas(id,'test/data/telemac2d/malpasset/t2d_malpasset-fine.cas')
  # output=api.run(id,api=True)
  # api.run("calibration/test",api=True)
  

def test_ApiTelemac_Run_Docker():
  # id="confluencerun/t2d_confluencev1"
  # api.Docker(id,vcpus=2)
  
  api.Docker("calibration/test",vcpus=4)
  
def test_ApiTelemac_Run_AWS():
  # api.AWS("confluencerun/t2d_confluencev2",cas='test/data/telemac2d/confluence/t2d_confluence.cas',vcpus=4)
  api.AWS("calibration/test",vcpus=4)
  
  

if __name__ == "__main__":
  # test_AWSOpenTelemac()
  # test_ApiTelemac_Run()
  # test_ApiTelemac_RunParallel()
  # test_ApiTelemac_Run_Docker()
  test_ApiTelemac_Run_AWS()
  
  