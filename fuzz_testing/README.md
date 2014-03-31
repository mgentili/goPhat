Fuzz Testing (Work in Progress)
======

Make sure that `goPhat/fuzz_testing/fuzz_testing_exec.go` has been installed.

From the `fuzz_testing` directory, run the following on the command line

    go install
    fuzz_testing --path [filename]

where [filename] is the name of the file that contains the randomly generated
data to be used for the test. The first line should be the number of replicas
to create, and each following line should be
an integer between 0 and 30. Currently only killing a replica (&lt;10), stopping
a replica (&lt;20), and putting data work.

To stop the nodes afterwards, run 
	kill_all fuzz_testing_exec