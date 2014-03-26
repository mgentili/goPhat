Fuzz Testing (Work in Progress)
======

Make sure that goPhat/vr/vr_exec.go has been installed.

From the fuzz_testing directory, run the following on the command line

~~~
go install
fuzz_testing --path [filename]
~~~
where [filename] is the name of the file that contains the randomly generated
data to be used for the test. The first line should be the number of replicas
to create (currently only 3 works properly), and each following line should be
an integer between 0 and 20. Currently only killing a replica (<10) and stopping
a replica (<20) work.

