#!/usr/bin/env bash
set -ex

FILES=()
for i in $( git log -m -1 --name-only --pretty="format:" $1 ); do
    # Remove all the prefix until the "/" character
    FILENAME=${i##*/}

    # Remove all the prefix until the "." character
    FILEEXTENSION=${FILENAME##*.}

    if [ $FILEEXTENSION = "py" ]; then
        FILES+=( "$i" )
    fi
done
#echo "${FILES[@]}"

CMDS=()
for i in "${FILES[@]}"; do
    CMDS+=("--include=$i")
done
echo "${CMDS[@]}"