#!/bin/sh
# build binary distributions for linux/amd64 and darwin/amd64
set -eu

cd "$(dirname "$0")"
DIR=$(pwd)
echo "working dir $DIR"
mkdir -p $DIR/dist

arch=$(go env GOARCH)
version=$(awk '/const VERSION/ {print $NF}' $DIR/version.go | sed 's/"//g')
goversion=$(go version | awk '{print $3}')

echo "... running tests"
./test.sh

for os in linux darwin freebsd; do
    echo "... building v$version for $os/$arch"
    BUILD=$(mktemp -d ${TMPDIR:-/tmp}/statsdaemon.XXXXXX)
    TARGET="statsdaemon-$version.$os-$arch.$goversion"
    GOOS=$os GOARCH=$arch CGO_ENABLED=0 go build -o $BUILD/$TARGET/statsdaemon
    cd $BUILD
    tar czvf $TARGET.tar.gz $TARGET
    mv $TARGET.tar.gz $DIR/dist
    cd $DIR
    rm -r $BUILD
done
