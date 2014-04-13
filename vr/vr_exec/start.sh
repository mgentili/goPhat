#!/bin/sh

go install && (killall vr_exec; 
vr_exec -r=4 &
vr_exec -r=3 &
vr_exec -r=2 & vr_exec -r=0 & true) && vr_exec -r=1