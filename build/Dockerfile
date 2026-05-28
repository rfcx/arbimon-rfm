FROM python:3.8-buster AS arbimon-rfm
RUN set -eux; \
    sed -i 's|http://deb.debian.org/debian|http://archive.debian.org/debian|g; s|http://deb.debian.org/debian-security|http://archive.debian.org/debian-security|g; s|http://security.debian.org/debian-security|http://archive.debian.org/debian-security|g' /etc/apt/sources.list; \
    printf 'Acquire::Check-Valid-Until "false";\nAcquire::Retries "5";\n' > /etc/apt/apt.conf.d/99archive

# Install sox and libsndfile
RUN apt-get update && \
    apt-get install -y opus-tools sox libsndfile-dev python3-opencv

# Install Python dependencies
ADD requirements.txt /requirements.txt
RUN pip3 install --no-cache-dir --upgrade pip && \
    pip3 install --no-cache-dir autoflake8 flake8 bandit && \
    pip3 install --no-cache-dir numpy==1.24.4 && \
    pip3 install --no-cache-dir -r /requirements.txt

ADD build/entrypoint.sh /entrypoint.sh
RUN chmod 700 /entrypoint.sh && mkdir /app
ENTRYPOINT [ "/entrypoint.sh" ]

ARG RFCX_API_URL=https://staging-api.rfcx.org/
ENV RFCX_API_URL=${RFCX_API_URL}

WORKDIR /app
ADD rfm rfm

CMD ["cli"]
