#!/bin/sh

go install && (killall vr_exec; vr_exec -r=2 & vr_exec -r=1 & true) && vr_exec