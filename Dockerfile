FROM ubuntu:trusty

RUN apt-get update && apt-get install -y gcc g++ python-dev python-pip

RUN pip install pyzmq==14.0.1 --install-option="--zmq=bundled"

RUN pip install ssbench 
