#!/bin/sh

test_type=$1

./install.sh
fuzz_testing --test $test_type