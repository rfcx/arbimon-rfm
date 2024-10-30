FROM ubuntu
MAINTAINER Giovany Vega <aleph.omega@gmail.com>
LABEL Description="Jobs container" \
      Vendor=Arbimon2 \
      Version=1.0

ENV DB__TIMEZONE=Z \
    APP_PATH=/app/jobs \
    SCRIPT_PATH=scripts

RUN mkdir /root/.ssh/

RUN apt-get update -y
RUN apt-get install -y \
    bwidget \
    gfortran \
    libfftw3-3 \
    libfftw3-dev \
    libfreetype6-dev \
    liblapack-dev \
    libmysqlclient-dev \
    libopenblas-dev \
    libpng12-dev \
    libsndfile1 \
    libsndfile-dev \
    libsamplerate-dev \
    python-dev \
    python-opencv \
    python-pip \
    python-virtualenv \
    r-base \
    r-base-dev \
    r-cran-rgl

COPY requirements.txt /app/requirements.txt
COPY scripts /app/scripts

WORKDIR /app/

RUN scripts/setup/setup.sh

COPY . /app/


CMD ["/bin/sh"]
