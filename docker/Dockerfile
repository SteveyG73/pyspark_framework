FROM openjdk:8-alpine

RUN echo "**** install bash ****" && \
    apk update && \
    apk add bash

RUN echo "**** install Python ****" && \
    apk add --no-cache python3 && \
    if [ ! -e /usr/bin/python ]; then ln -sf python3 /usr/bin/python ; fi && \
    \
    echo "**** install pip ****" && \
    python3 -m ensurepip && \
    rm -r /usr/lib/python*/ensurepip && \
    pip3 install --no-cache --upgrade pip setuptools wheel && \
    if [ ! -e /usr/bin/pip ]; then ln -s pip3 /usr/bin/pip ; fi

RUN echo "**** install pyspark ****" && \
    pip3 install --user pyspark==2.4.4

RUN echo "**** install pyspark ****" && \
    pip3 install --user boto3~=1.10

ENV SPARK_HOME=/root/.local/lib/python3.6/site-packages/pyspark

RUN echo "**** install livy *****" && \
    mkdir /opt/apache-livy && \
    wget "http://www.mirrorservice.org/sites/ftp.apache.org/incubator/livy/0.6.0-incubating/apache-livy-0.6.0-incubating-bin.zip" \
         -O /tmp/livy.zip && \
    unzip /tmp/livy.zip -d /opt/apache-livy
RUN mkdir /opt/apache-livy/apache-livy-0.6.0-incubating-bin/logs

ADD docker/conf/livy.conf /opt/apache-livy/apache-livy-0.6.0-incubating-bin/conf

EXPOSE 8998

ADD docker/entrypoint.sh /app/entrypoint.sh
ADD docker/test.py /app
ADD pyspark_framework /app/pyspark_framework
ADD requirements.txt /app

RUN pip3 install -r /app/requirements.txt

WORKDIR /app
ENTRYPOINT ["/app/entrypoint.sh"]


