#!/bin/sh

test_type=$1

cd fuzz_testing_exec
go install
cd ../
go install
fuzz_testing --test $test_type