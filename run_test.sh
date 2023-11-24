#! /bin/bash
set -e

sudo ./init_test.sh
sudo ./ramon_512.sh
sudo ./set_trace.sh
sleep 1
cat /sys/fs/cgroup/yuri/memcached_server/memory.stat > startmemstat.txt

#start
sudo sh -c "echo 1 > /sys/kernel/debug/tracing/tracing_on"
python /home/yuri/workloads/memcached/client_run.py > log.file 2>&1 & 
sudo cat /sys/kernel/debug/tracing/trace_pipe > trace_record_p.txt &

sleep 1000
#end
sudo sh -c "echo 0 > /sys/kernel/debug/tracing/tracing_on"
cat /sys/fs/cgroup/yuri/memcached_server/memory.stat > endmemstat.txt

vmhgfs-fuse .host:/ /mnt/hgfs
mv startmemstat.txt /mnt/hgfs/sharefolders/startmemstat.txt.memcached
mv endmemstat.txt /mnt/hgfs/sharefolders/endmemstat.txt.memcached
mv trace_record_p.txt /mnt/hgfs/sharefolders/trace_record_p.txt.memcached