#!/bin/bash
# Inspired by http://ubuntuforums.org/showthread.php?t=910717.
set -o errexit

if [ "$#" -ne 2 ]; then
  echo "$0 <version> <386|amd64>"
  echo
  echo "where version is something like 'X.Y-Z'."
  exit 1
fi

VERSION=$1
BASEDIR=statsdaemon_$VERSION
ARCH=$2

GOOS=linux GOARCH=$ARCH go build statsdaemon.go

if [ -d $BASEDIR ];then
  rm -frv $BASEDIR
fi
cp -r deb $BASEDIR
mkdir -pv $BASEDIR/usr/local/bin
cp -v statsdaemon $BASEDIR/usr/local/bin

sed "s/VERSION/$VERSION/g" deb/DEBIAN/control | sed "s/ARCH/$ARCH/g" > $BASEDIR/DEBIAN/control

if [ -e ${BASEDIR}.deb ];then
  rm -v ${BASEDIR}.deb
fi
dpkg-deb --build $BASEDIR
