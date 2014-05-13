from __future__ import with_statement
from fabric.api import *

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

env.roledefs = {
    'servers': ['54.186.231.172','54.187.133.58','54.187.131.11'],
    'client': ['54.187.135.8']
} 

server_private = '172.31.25.80:9000 172.31.25.82:9000 172.31.25.79:9000'
server_rpcs = '172.31.25.80:1337 172.31.25.82:1337 172.31.25.79:1337'
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
	run('ping -c 10 172.31.25.80')

@parallel
@roles('servers')
def start_server():
	run('qserver --servers "{}"'.format(server_private))

@roles('client')
def start_client():
	run('windowed --num_messages 1000 --window_size 10 --servers "{}"'.format(server_rpcs))

#172.31.25.80,172.31.25.82,172.31.25.79,172.31.25.81

