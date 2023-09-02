FROM opensuse/tumbleweed:latest

ARG GOPATH=/gopath
ARG GOROOT=/goroot

ENV GOROOT=${GOROOT}
ENV GOPATH=${GOPATH}

# Install required tools and Python 3.9
RUN zypper --non-interactive update && \
    zypper --non-interactive install --no-recommends --force-resolution \
    iperf \
    tar \
    gzip \
    gcc \
    gcc-c++ \
    wget \
    libgthread-2_0-0 \
    python39 \
    python39-pip

# Install required python modules
RUN python3.9 -m pip --no-cache install \
  tornado==6.1.0 \
  jsonpath-python==1.0.5 \
  oauthlib==3.1.0 \
  requests-oauthlib==1.3.1 \
  pandas==1.2.5 \
  pyarrow==12.0.1 \
  iperf3 \
  u-msgpack-python==2.7.1

# Install required golang version
RUN wget --no-check-certificate -q -O /tmp/sapgolang.tar.gz  https://go.dev/dl/go1.19.1.linux-amd64.tar.gz

RUN mkdir -p ${GOROOT} && \
    tar -xzf /tmp/sapgolang.tar.gz -C ${GOROOT}

# Configure GOROOT and GOPATH (golang required)
ENV GOROOT=${GOROOT}/go
ENV GOPATH=${GOPATH}/go
ENV PATH=${GOROOT}/bin:${GOPATH}/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

RUN groupadd -g 1972 vflow && useradd -g 1972 -u 1972 -m vflow
USER 1972:1972
WORKDIR /home/vflow
ENV HOME=/home/vflow
