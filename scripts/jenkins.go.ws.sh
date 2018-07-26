#!/bin/bash

#Using s3://eyeview-deploy/tools/go1.4.1.linux-amd64.tar.gz  aka https://storage.googleapis.com/golang/go1.4.1.linux-amd64.tar.gz

#export GOROOT=/opt/go_1.8
export GOROOT=/opt/go
export GOPATH=`pwd`
export PATH=$GOROOT/bin:$PATH
echo "gopath:"
echo $GOPATH

#HACK: It doesn't look like there's a way to change the git local directory for checkout so moving files manually for now
echo "cleaning src/github.com dir"
rm -rf src/github.com || /bin/true
mkdir -p src/github.com/eyeview/statsdaemon || /bin/true
find . -maxdepth 1|egrep -v '.git|src|bin|pkg|deploy.|^.$|workspace'|xargs mv -f -t src/github.com/eyeview/statsdaemon/

cd src/github.com/eyeview/statsdaemon/
${GOROOT}/bin/go build
#${GOROOT}/bin/go test ./...

