FROM alpine:3.15

RUN apk add --no-cache bash subversion
RUN mkdir /telemac && \
    svn co http://svn.opentelemac.org/svn/opentelemac/tags/v8p1r0/scripts /telemac/scripts --username=ot-svn-public --password=telemac1* && \
    svn co http://svn.opentelemac.org/svn/opentelemac/tags/v8p1r0/sources /telemac/sources --username=ot-svn-public --password=telemac1* && \
    svn co http://svn.opentelemac.org/svn/opentelemac/tags/v8p1r0/optionals/metis-5.1.0 /telemac/metis --username=ot-svn-public --password=telemac1*


RUN apk add --no-cache \
    -X http://dl-cdn.alpinelinux.org/alpine/v3.15/main \
    -X http://dl-cdn.alpinelinux.org/alpine/v3.15/community \
    -X http://dl-cdn.alpinelinux.org/alpine/v3.15/testing \
    util-linux musl musl-dev libc-dev zlib numactl xz-libs eudev-libs libxml2 \
    git openssh-client openssh-server g++ gfortran make cmake pkgconf openmpi openmpi-dev \
    python3 py3-numpy py3-matplotlib py3-numpy-dev py3-numpy-f2py \
    py3-boto3 py3-scipy py3-h5py hdf5 hdf5-dev netcdf netcdf-dev
    

RUN echo "**** install Python ****" && \
    if [ ! -e /usr/bin/python ]; then ln -sf python3 /usr/bin/python ; fi && \
    \
    echo "**** install pip ****" && \
    python3 -m ensurepip && \
    rm -r /usr/lib/python*/ensurepip && \
    pip3 install --no-cache --upgrade pip setuptools wheel && \
    if [ ! -e /usr/bin/pip ]; then ln -s pip3 /usr/bin/pip ; fi

RUN pip3 install --no-cache awscli mpi4py requests tqdm

COPY ./systel.cfg /telemac/systel.cfg

RUN cd /telemac/metis && \
    cmake -D CMAKE_INSTALL_PREFIX='/telemac/metis' . && \
    make && make install

ENV PYTHONUNBUFFERED=1
ENV HOMETEL="/telemac"
ENV SYSTELCFG="/telemac/systel.cfg"
ENV PATH="/telemac/scripts/python3:$PATH"
ENV USETELCFG="gfortranp"
ENV PYTHONPATH="$HOMETEL/scripts/python3:$PYTHONPATH"
ENV LD_LIBRARY_PATH="$HOMETEL/builds/$USETELCFG/wrap_api/lib:$LD_LIBRARY_PATH"
ENV PYTHONPATH="$HOMETEL/builds/$USETELCFG/wrap_api/lib:$PYTHONPATH"
ENV METISHOME="/telemac/metis"

RUN compile_telemac.py

RUN HDF5_LIBDIR=/usr/lib HDF5_INCDIR=/usr/include python3 -m pip --no-cache-dir install netcdf4

RUN mkdir /packages
RUN mkdir /data
RUN mkdir /working

WORKDIR /working
ENV OMPI_MCA_btl_vader_single_copy_mechanism=none
ENV TELEMAC_LOCALFOLDER="/data"
ENV HDF5_DISABLE_VERSION_CHECK="1"
ENV AWS_CACHELOCATION="/data"



COPY ./packages/slf-py /packages/slf-py
COPY ./packages/aws-opentelemac /packages/aws-opentelemac
COPY ./packages/aws-opentelemac/runapi.py /working/runapi.py

RUN pip install packaging
RUN pip install --no-cache /packages/slf-py 
RUN pip install --no-cache /packages/aws-opentelemac

COPY ./run.py /working/run.py

CMD ["python","run.py"]