FROM alpine:latest
RUN sh -c "echo $RANDOM > /etc/random.txt"