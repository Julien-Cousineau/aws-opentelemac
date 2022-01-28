


sudo mount /dev/nvme1n1 /home/ec2-user/environment/cccris/ebs/

```bash
conda create -y -c conda-forge -n tara2 python=3.10
conda activate tara2
# IMPORTANT numpy >=1.22.0 does not work!
conda install -y -c conda-forge boto3 numpy=1.21.5 tqdm matplotlib
conda install -y -c conda-forge libgfortran
conda install -y -c conda-forge openmpi openmpi-mpifort openmpi-devel
conda install -y -c conda-forge mpi4py
conda install -y -c conda-forge make cmake
conda install -y-c conda-forge halo

pip install -e ./slf-py
pip install -e ./aws-opentelemac


svn co http://svn.opentelemac.org/svn/opentelemac/tags/v8p1r0/scripts ./opentelemac/v8p1r0/scripts --username=ot-svn-public --password=telemac1*
svn co http://svn.opentelemac.org/svn/opentelemac/tags/v8p1r0/sources ./opentelemac/v8p1r0/sources --username=ot-svn-public --password=telemac1*
svn co http://svn.opentelemac.org/svn/opentelemac/tags/v8p1r0/optionals/metis-5.1.0 ./opentelemac/v8p1r0/metis --username=ot-svn-public --password=telemac1*

cd opentelemac/v8p1r0/metis
cmake -D CMAKE_INSTALL_PREFIX='/home/ec2-user/environment/aws-opentelemac/opentelemac/v8p1r0/metis' -DSHARED=TRUE .
make && make install

compile_telemac.py


cd $CONDA_PREFIX
mkdir -p ./etc/conda/activate.d
mkdir -p ./etc/conda/deactivate.d

touch ./etc/conda/activate.d/env_vars.sh
vi ./etc/conda/activate.d/env_vars.sh
export SYSTELCFG="/home/ec2-user/environment/aws-opentelemac/opentelemac/systel.cfg"
export PATH="/home/ec2-user/environment/aws-opentelemac/opentelemac/v8p1r0/scripts/python3:$PATH"
export HOMETEL="/home/ec2-user/environment/aws-opentelemac/opentelemac/v8p1r0"
export USETELCFG=gfortranp
export PYTHONPATH=$HOMETEL/scripts/python3:$PYTHONPATH
export PYTHONPATH=$HOMETEL/builds/$USETELCFG/wrap_api/lib:$PYTHONPATH
export METISHOME=$HOMETEL/metis
export LD_LIBRARY_PATH=$HOMETEL/builds/$USETELCFG/wrap_api/lib:$METISHOME/lib


touch ./etc/conda/deactivate.d/env_vars.sh
vi ./etc/conda/deactivate.d/env_vars.sh
unset SYSTELCFG
unset HOMETEL
unset USETELCFG
unset LD_LIBRARY_PATH
unset PYTHONPATH
```bash

