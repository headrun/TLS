#!/bin/sh

. $(dirname "$0")/base.sh

validate() {
    v=`git diff origin/HEAD $1 | head -n 1`
    if [ "$v" != "" ]
    then
        echo "Error: Uncommited local changes. run git status; git log --decorate --oneline"
        exit 1
    fi
}

create_release_dir() {
    rel_dir=$1

    rm -rf $rel_dir

    files=`git ls-files | grep '.\(py\|cfg\|sh\|conf\|ini\)$' | grep -v pylint.sh`
    validate "$files"

    for x in $files
    do
        d=$rel_dir/$(dirname $x)
        mkdir -p $d
        cp $x $d/
    done
}

main() {
    release_root=$1
    if [ -z $release_root ] || [ ! -d $release_root ]
    then
        echo "Invalid arguments. release.sh <path_for_releases>"
        exit 1
    fi

    branch=`git branch -v | cut -f2 -d' '`
    version=`git branch -v | cut -f3 -d' '`

    release_dir=${g_project}_${branch}_${g_timestamp}_${version}
    cd $g_project_dir
    create_release_dir $release_root/$release_dir

    cd $release_root
    tar -czf $release_dir.tgz $release_dir
    rm -rf $release_dir
    echo "Release: $PWD/${release_dir}.tgz"
}

main $1
