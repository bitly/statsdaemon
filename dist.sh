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

    pushd $BUILD >/dev/null
    tar czvf $TARGET.tar.gz $TARGET
    if [ -e $DIR/dist/$TARGET.tar.gz ]; then
        echo "... WARNING overwriting dist/$TARGET.tar.gz"
    fi
    mv $TARGET.tar.gz $DIR/dist
    echo "... built dist/$TARGET.tar.gz"
    popd >/dev/null
    rm -r $BUILD
done
