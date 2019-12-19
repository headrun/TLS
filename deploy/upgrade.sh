#!/bin/sh

. $(dirname "$0")/base.sh

crawler() {
    release_dir=$g_project_dir

    cd $ENV_HOME
    ln -sfn $release_dir $ENV_PROJECT
}

main() {
	crawler
}


main $1
