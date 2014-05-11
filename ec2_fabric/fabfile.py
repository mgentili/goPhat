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


@parallel
def setup():
  sudo('sudo yum update -y')
  sudo('sudo yum install golang -y')
  put('setup.sh', '/tmp/setup.sh', mirror_local_mode=True)
  run('/tmp/setup.sh')


@parallel
def start_server(servers):
  run('qserver --servers "{}"'.format(servers))