#!/bin/bash
bin=`dirname "$0"`
bin=`cd "$bin";pwd`
if [ -f "$bin"/.pid_infer.txt_ ]; then
"$bin"/stop-infer_server.sh
fi
echo "`date +"%Y-%m-%d %H:%M:%S"` INFO: start INFER SERVER ... ..." >> "$bin"/.runtime.log 2>&1 

#. "$bin"/set_env.sh

pushd "$bin" > /dev/null
python "$bin"/Server.py >> "$bin"/.runtime.log  2>&1 & 
popd > /dev/null
echo $! > "$bin"/.pid_infer.txt_

sleep 5 > /dev/null
echo -e '\tINFER SERVER is successfully started!' >>"$bin"/.runtime.log  2>&1
