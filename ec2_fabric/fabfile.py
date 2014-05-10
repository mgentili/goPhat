from __future__ import with_statement
from fabric.api import *
from fabric.contrib.console import confirm

env.hosts = ['54.187.17.13','54.187.91.175','54.187.80.101']
#

def test():
	do_test_stuff()

def ec2():
	env.user = 'ec2-user'
	env.key_filename = '/home/mgentili/Dropbox/AWS-VM.pem'

def setup():
	ec2()
	script_file = open('setup.sh')
	run(script_file.read())
	script_file.close()

def test():
	ec2()
	sudo("cd ")	
