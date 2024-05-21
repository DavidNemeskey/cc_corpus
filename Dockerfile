FROM ubuntu:22.04

RUN apt-get update && apt-get -y upgrade \
    && apt-get install -y --no-install-recommends \
    build-essential \
    ca-certificates \
    wget \
    parallel \
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
# RUN rm 'cc_corpus/app/sql_app.db'
RUN . ${HOME}/.bashrc && conda activate cc_corpus && conda install python=3.11 pip \
    && export CFLAGS="-Wno-narrowing" \
    && pip install -e cc_corpus

RUN echo 'conda activate cc_corpus ' >> ~/.bashrc

WORKDIR "${HOME}/cc_corpus"

RUN cp app/config_docker.yaml app/config.yaml
RUN echo '#!/bin/bash' >> launch_app.sh \
    && echo 'source ~/miniconda3/etc/profile.d/conda.sh' >> launch_app.sh \
    && echo 'conda activate cc_corpus' >> launch_app.sh \
    && echo 'uvicorn app.main:app --host 0.0.0.0 --port 8000' >> launch_app.sh
RUN chmod +x launch_app.sh

EXPOSE 8000
# The port used by uvicorn for the webserver.

ENTRYPOINT ["/bin/bash", "-l", "-c"]

CMD ["./launch_app.sh"]
# CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
# CMD ["/bin/bash", "-c", "conda activate cc_corpus && uvicorn app.main:app --host 0.0.0.0 --port 8000"]
