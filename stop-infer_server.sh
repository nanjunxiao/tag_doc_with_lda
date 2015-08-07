#!/bin/bash
bin=`dirname "$0"`
bin=`cd "$bin";pwd`
for i in `cat "$bin"/.pid_infer.txt_`;
do
        ps awx -o "%p %P"|grep -w $i| awk '{ print $1 }'|xargs kill -9
	echo "`date +"%Y-%m-%d %H:%M:%S"`, INFO: INFER SERVER $i is stopped" 2>&1 | tee -a "$bin"/.runtime.log
done;

rm -rf "$bin"/.pid_infer.txt_
