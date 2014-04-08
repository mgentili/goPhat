#!/bin/sh

cd fuzz_testing_exec
go install
cd ../
go install
fuzz_testing --test 3