FROM python:3.8-buster AS arbimon-rfm

# Install sox and libsndfile
RUN apt-get update && \
    apt-get install -y sox libsndfile-dev python3-opencv

# Install Python dependencies
ADD requirements.txt /requirements.txt
RUN pip3 install --no-cache-dir --upgrade pip && \
    pip3 install --no-cache-dir autoflake8 flake8 bandit && \
    pip3 install --no-cache-dir -r /requirements.txt

ADD build/entrypoint.sh /entrypoint.sh
RUN chmod 700 /entrypoint.sh && mkdir /app
ENTRYPOINT [ "/entrypoint.sh" ]

ARG RFCX_API_URL=https://staging-api.rfcx.org/
ENV RFCX_API_URL=${RFCX_API_URL}

WORKDIR /app
ADD rfm rfm

CMD ["cli"]
