FROM ubuntu:22.04

SHELL ["/bin/bash", "-c"]

WORKDIR /

ENV TZ="Etc/UTC"
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

RUN DEBIAN_FRONTEND="noninteractive" apt-get update -y && apt-get install -y \
    bison \
    build-essential \
    cmake \
    curl \
    doxygen \
    flex \
    gfortran \
    git \
    libarmadillo-dev \
    libboost-date-time-dev \
    libboost-filesystem-dev \
    libboost-program-options-dev \
    libboost-python-dev \
    libboost-system-dev \
    libboost-test-dev \
    libcfitsio-dev \
    libfftw3-dev \
    libgsl-dev \
    libgtkmm-3.0-dev \
    libhdf5-serial-dev \
    liblapack-dev \
    liblua5.3-dev \
    libpng-dev \
    ninja-build \
    pipx \
    pkg-config \
    pybind11-dev \
    python-is-python3 \
    python3-dev \
    python3-numpy \
    python3-pip \
    wcslib-dev\
    wget

# Install WSRT Measures (extra casacore data, for integration tests)
# Note: The file on the ftp site is updated daily. When warnings regarding leap
# seconds appear, ignore them or regenerate the docker image.
RUN mkdir -p /usr/share/casacore/data && \
    ln -s /usr/share/casacore /var/lib/casacore && \
    wget -qO - ftp://ftp.astron.nl/outgoing/Measures/WSRT_Measures.ztar | \
    tar -C /usr/share/casacore/data -xzf -

# Build CasaCore
ENV CASACORE_VERSION=v3.6.0
RUN git clone https://github.com/casacore/casacore.git casacore && \
    cd casacore && git checkout ${CASACORE_VERSION} && \
    mkdir build && cd build && \
    cmake -DUSE_OPENMP=ON -DBUILD_PYTHON3=ON -DUSE_HDF5=ON -DUSE_THREADS=ON \ 
          -DBUILD_TESTING=OFF -DDATA_DIR=/usr/share/casacore/data .. && \
    make -j `nproc` && make install && \
    cd / && rm -rf casacore

# Build AOFlagger3
ENV AOFLAGGER_VERSION=b1256de90b00a5a83477274390decd6671cdcd38
RUN git clone https://gitlab.com/aroffringa/aoflagger.git aoflagger && \
    cd aoflagger && git checkout ${AOFLAGGER_VERSION} && \
    mkdir build && cd build && \
    cmake -G Ninja .. && ninja install && \
    cd / && rm -rf aoflagger

# Build IDG
ENV IDG_VERSION=9ce6fa88b9d746d8d7146c474992aba9b98eb41f
RUN git clone https://git.astron.nl/RD/idg.git idg && \
    cd idg && git checkout ${IDG_VERSION} && \
    mkdir build && cd build && \
    cmake -G Ninja -DPORTABLE=ON .. && ninja install && \
    cd / && rm -rf idg

# Build EveryBeam
ENV EVERYBEAM_VERSION=0578473cacf64c69bc2e05e15754cf94dd1051b9
RUN git clone https://git.astron.nl/RD/EveryBeam.git everybeam && \
    cd everybeam && git checkout ${EVERYBEAM_VERSION} && \
    mkdir build && cd build && \
    cmake -G Ninja .. && ninja install && \
    cd / && rm -rf everybeam

# Build DP3
ENV DP3_VERSION=v6.3
RUN git clone https://github.com/lofar-astron/DP3.git dp3 && \
    cd dp3 && git checkout ${DP3_VERSION} && \
    mkdir build && cd build && \
    cmake -DPORTABLE=ON .. && \
    make -j `nproc` && make install && \
    cd / && rm -rf dp3

RUN pipx ensurepath
ENV PATH="/root/.local/bin:${PATH}"
# Install poetry
RUN curl -sSL https://install.python-poetry.org | python3 -
