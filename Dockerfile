FROM golang:1.11

# Set the Current Working Directory inside the container
WORKDIR $GOPATH/src/statsdaemon/

# Copy everything from the current directory to the PWD(Present Working Directory) inside the container
COPY . .

# Download all the dependencies
RUN go get -d -v ./...

# Install the package
RUN go install -v ./...

# Expose required ports
EXPOSE 8125/udp 8126 

# Run the executable
CMD ["statsdaemon", "-prefix=e0858dfc-0e6d-4004-8393-844a5ddda73a.stats.", "-graphite=carbon.hostedgraphite.com:2003,127.0.0.1:2003"]