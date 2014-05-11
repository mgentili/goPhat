#!/bin/sh

sudo yum install golang

mkdir -p $HOME/gopath/src

profile_file="$HOME/.bash_profile"

if ! grep -q 'GOPATH' $HOME/.bash_profile ; then
	echo -e "export GOPATH=\$HOME/gopath" >> $HOME/.bash_profile
	source $HOME/.bash_profile
fi

if ! grep -q 'GOPATH/bin' $HOME/.bash_profile ; then
	echo -e "export PATH=$PATH:\$GOPATH/bin" >> $HOME/.bash_profile
	source $HOME/.bash_profile
fi

mkdir -p $GOPATH/src/github.com/mgentili
cd $GOPATH/src/github.com/mgentili
sudo rm -rf goPhat
git clone git://github.com/mgentili/goPhat
