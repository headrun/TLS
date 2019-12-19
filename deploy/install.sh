#!/bin/sh

. $(dirname "$0")/base.sh

main() {
    release_tgz=$1
    if [ -z $release_tgz ] || [ ! -f $release_tgz ]
    then
        echo "Invalid file $release_tgz"
        exit 1
    fi
    tar -xzf $release_tgz
    release_dir=`echo ${release_tgz##*\/} | cut -f1 -d.`
    sh $release_dir/deploy/upgrade.sh
}

main $1
