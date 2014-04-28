#!/bin/bash
# arguments

if [ 3 -ne $# ]; then
    echo "Usage: $0 F seed runs" 1>&2
    exit 1
fi

F=$1
SEED=$2
RUNS=$3
N=$((F * 2 + 1))

LOC=$(mktemp /tmp/gophat.XXXXXXX)
if [ 0 -ne $? ]; then
echoerr "Could not create a temporary file, cannot complete"
    exit 1
fi

# functions

# timed blocks

function timer_total() {
    # $RANDOM generates numbers in [0, 32767]
    for i in `seq 1 $RUNS`
    do
        R=$RANDOM
        CHILD_SEED=$RANDOM
        if [ $R -le 10000 ];
        then
            echo "createfile" >> $LOC
        elif [ $R -le 15000 ];
        then
            echo "stopnode" $((CHILD_SEED % N)) >> $LOC
        elif [ $R -le 30000 ];
        then
            echo "wait" $((CHILD_SEED % 1000)) >> $LOC
        else
            echo "resumenode" $((CHILD_SEED % N)) >> $LOC
        fi
    done
}

# executable portion
echo "startnodes" $N 1 > $LOC
RANDOM=$SEED
timer_total

#time timer_total

./install.sh && fuzz_testing --path $LOC && rm -f $LOC