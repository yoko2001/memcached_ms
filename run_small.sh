#! /bin/bash
#set -e

sudo ./init_test.sh
sleep 1

sudo ./ramon_2048.sh
sudo swapoff /dev/dm-1
sudo ./set_trace.sh
sleep 1
cat /sys/fs/cgroup/yuri/memcached_server/memory.stat > startmemstat.txt

#start
sudo sh -c "echo 1 > /sys/kernel/debug/tracing/tracing_on"
python ./client_run_small.py > log.txt 2>&1 & 

sudo sh -c "cat /sys/kernel/debug/tracing/trace_pipe > trace_record_p.txt &"

sleep 240
#end
sudo sh -c "echo 0 > /sys/kernel/debug/tracing/tracing_on"

cat /sys/fs/cgroup/yuri/memcached_server/memory.stat > endmemstat.txt

#sudo ./mv.sh
