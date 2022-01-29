import os
from telapy.api.t2d import Telemac2d
from telapy.api.t3d import Telemac3d
from telapy.api.wac import Tomawac
from telapy.api.sis import Sisyphe
from mpi4py import MPI
import numpy as np

from awsopentelemac import AWSOpenTelemac
import argparse

modules = {
  "telemac2d":Telemac2d,
  "telemac3d":Telemac3d,
  "tomawac":Tomawac,
  "sisyphe":Sisyphe
}



def parseOptions(comm):
    parser = argparse.ArgumentParser(description='Run telapy')
    parser.add_argument('id', help='Cas id', type=str)
    parser.add_argument('module', help='Module', type=str)
    args = None
    try:
      if comm.Get_rank() == 0:
          args = parser.parse_args()
    finally:
        args = comm.bcast(args, root=0)

    if args is None:
        exit(0)
    return args

def run():
  comm = MPI.COMM_WORLD
  rank = comm.Get_rank()
  size = comm.Get_size()
  args = parseOptions(comm)
  id   = args.id
  module   = args.module
  
  recompile=None
  if rank==0:
    api=AWSOpenTelemac()
    item=api.get(id)
    keywords=item.get("keywords")
    recompile=bool(keywords.get("FORTRAN FILE",False))
    
  recompile = comm.bcast(recompile, root=0)
  user_fortran='user_fortran' if recompile else None
  
  study = modules[module]("temp.cas", user_fortran=user_fortran,comm=comm, stdout=0,recompile=recompile)
  study.set_case()
  study.init_state_default()
  
  igprintout = study.cas.values.get("GRAPHIC PRINTOUT PERIOD",study.cas.dico.data["GRAPHIC PRINTOUT PERIOD"])
  ntimesteps = study.get("MODEL.NTIMESTEPS")
  
  if rank==0:api.updateProgress(id,iframe=0,nframe=int(ntimesteps/igprintout))
  for itime in range(0,ntimesteps):
    study.run_one_time_step()
    itime1=itime+1
    if rank==0 and itime1%igprintout==0:
      print("Frame at step={} of nstep={}".format(itime1,ntimesteps))
      api.updateProgress(id,iframe=np.floor(float(itime1)/igprintout),nframe=int(ntimesteps/igprintout))
  
  
  study.finalize()
  del study
  
if __name__ == "__main__":
  run()