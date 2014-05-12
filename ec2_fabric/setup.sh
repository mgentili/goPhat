#!/bin/bash

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

go get -u github.com/mgentili/goPhat/benchmarks/qserver
go get -u github.com/mgentili/goPhat/benchmarks/windowed
