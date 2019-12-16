FROM amazonlinux:2018.03.0.20191014.0

RUN yum -y install fuse
COPY --from=golang:1.13 /usr/local/go /usr/local/go
ENV PATH="/usr/local/go/bin:${PATH}"
