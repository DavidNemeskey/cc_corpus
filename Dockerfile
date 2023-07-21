FROM ubuntu:22.04

RUN apt-get update && apt-get -y upgrade \
    && apt-get install -y --no-install-recommends \
    build-essential \
    ca-certificates \
    wget \
    && rm -rf /var/lib/apt/lists/*

ENV HOME /home/cc
RUN groupadd -r cc && useradd -r -g cc cc
RUN mkdir ${HOME} && chown cc:cc ${HOME}
USER cc
WORKDIR ${HOME}

ENV PATH="${HOME}/miniconda3/bin:${PATH}"
ARG PATH="${HOME}/miniconda3/bin:${PATH}"

RUN pwd && whoami && ls -la . && wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh \
    && mkdir ${HOME}/.conda \
    && bash Miniconda3-latest-Linux-x86_64.sh -b \
    && rm -f Miniconda3-latest-Linux-x86_64.sh \
    && echo "Running $(conda --version)" \
    && conda init bash \
    && . ${HOME}/.bashrc \
    && conda update conda \
    && conda create -n cc_corpus

Copy --chown=cc:cc . cc_corpus
RUN . ${HOME}/.bashrc && conda activate cc_corpus && conda install python=3.11 pip \
    && export CFLAGS="-Wno-narrowing" \
    && pip install -e cc_corpus

CMD ["/bin/bash"]
