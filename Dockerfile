FROM ubuntu:20.04

RUN apt-get update && apt-get -y upgrade \
    && apt-get install -y --no-install-recommends \
    build-essential \
    ca-certificates \
    wget \
    && rm -rf /var/lib/apt/lists/*

ENV PATH="/root/miniconda3/bin:${PATH}"
ARG PATH="/root/miniconda3/bin:${PATH}"

RUN wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh \
    && mkdir /root/.conda \
    && bash Miniconda3-latest-Linux-x86_64.sh -b \
    && rm -f Miniconda3-latest-Linux-x86_64.sh \
    && echo "Running $(conda --version)" \
    && conda init bash \
    && . /root/.bashrc \
    && conda update conda \
    && conda create -n cc_corpus \
    && conda activate cc_corpus

RUN conda install python=3.9 pip

Copy . /
RUN export CFLAGS="-Wno-narrowing" \
    && pip install -e .
