from __future__ import with_statement
from fabric.api import *
from fabric.contrib.files import exists
import time

# Define the hosts to run commands on via the command line:
# fab -H host1,host2 mytask

# If you already have all the SSH connection parameters in your ~/.ssh/config file,
# Fabric will natively support it
# http://docs.fabfile.org/en/1.4.0/usage/execution.html#ssh-config
env.use_ssh_config = True

# If you'd prefer otherwise, use:
# fab command -i /path/to/key.pem [-H [user@]host[:port]]
# For Marco, you would use:
# fab command -i /home/mgentili/Dropbox/AWS-VM.pem -H ec2-user@...

# env.roledefs = {
#     'servers': ['54.186.231.172','54.187.133.58','54.187.131.11'],
#     'client': ['54.187.135.8']
# }

env.roledefs = {
		'servers': ['54.187.234.238','54.187.237.77','54.187.237.3'],
    'client': ['54.186.130.98']
}

# server_private = '172.31.25.80:9000 172.31.25.82:9000 172.31.25.79:9000'
# server_rpcs = '172.31.25.80:1337 172.31.25.82:1337 172.31.25.79:1337'

server_private = ['172.31.13.142', '172.31.13.141', '172.31.13.140']
vrport = 9000
rpcport = 1337
env.user = 'ec2-user'
env.key_filename = '/home/mgentili/Dropbox/AWS-VM.pem'

@parallel
@roles('servers','client')
def setup():
	sudo('sudo yum update -y')
	sudo('sudo yum install golang -y')
	put('setup.sh', '/tmp/setup.sh', mirror_local_mode=True)
	run('/tmp/setup.sh')

@parallel
@roles('servers')
def shutdown():
	run('killall -9 qserver')

@roles('servers')
def ping():
	run('ping -c 5 172.31.13.142')
	run('ping -c 5 172.31.13.141')
	run('ping -c 5 172.31.13.140')


@parallel
@roles('servers')
def start_server(useVR = "true"):
	server_vr = " ".join(["{}:{}".format(i,vrport) for i in server_private])
	cmd = 'qserver --servers "{}" --vr {}'.format(server_vr, useVR)
	#run(cmd)
	run("nohup {} >& /dev/null < /dev/null &".format(cmd), pty=False)
	#run_bg('qserver --servers "{}" --vr {}'.format(server_vr, useVR))

@roles('client')
def start_client(num_messages = 1000, window_size = 1, output = "test.csv"):
	server_rpcs = " ".join(["{}:{}".format(i,rpcport) for i in server_private])
	run('windowed --num_messages {} --window_size {} --servers "{}" --file {}'.format(num_messages, window_size,server_rpcs,output))

def run_benchmark(useVR = "true", num_messages = 1000, window_size = 10):
	#execute(setup)
	execute(start_server,useVR)
	time.sleep(3)
	output_file = "{}messages_{}window_{}vr.csv".format(num_messages, window_size, useVR)
	run("rm -f {}".format(output_file))
	execute(start_client, num_messages, window_size, output_file)
	get(output_file, 'tmp/')
	execute(shutdown)

def run_bg(cmd, before=None, sockname="dtach", use_sudo=False):
    """Run a command in the background using dtach

    :param cmd: The command to run
    :param output_file: The file to send all of the output to.
    :param before: The command to run before the dtach. E.g. exporting
                   environment variable
    :param sockname: The socket name to use for the temp file
    :param use_sudo: Whether or not to use sudo
    """
    if not exists("/usr/bin/dtach"):
        sudo("yum install dtach -y")
    if before:
        cmd = "{}; dtach -n `mktemp -u /tmp/{}.XXXX` {}".format(
            before, sockname, cmd)
    else:
        cmd = "dtach -n `mktemp -u /tmp/{}.XXXX` {}".format(sockname, cmd)
    if use_sudo:
        return sudo(cmd)
    else:
        return run(cmd) 