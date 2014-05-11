Fuzz Testing (Work in Progress)
======

Make sure that `goPhat/fuzz_testing/fuzz_testing_exec.go` has been installed.
Modifying `install.sh` should be sufficient and will be run each time.

From the `fuzz_testing` directory, run the following on the command line:

    ./start 2m_o

Nodes will automatically be started and stopped according to the test spec.

To start a random test that is not human generated, run the following on the command line:

    ./random_test.sh 1 42 1

where the parameters are `F SEED RUNS` where `F` dictates the number of nodes (2F + 1),
`SEED` is the seed that will specify the operations in the test, and `RUNS` is the number of times run.
