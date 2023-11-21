#!/bin/bash
CGROUPNAME=memcached_server
cgdelete -r memory:/yuri/${CGROUPNAME}
cgdelete -r memory:/yuri/${CGROUPNAME}

if [ ! -d "/sys/fs/cgroup/yuri/" ];then
	mkdir /sys/fs/cgroup/yuri
else
	echo "cgroup yuri already exists"
fi
echo "+memory" >> /sys/fs/cgroup/yuri/cgroup.subtree_control

if [ ! -d "/sys/fs/cgroup/yuri/${CGROUPNAME}/" ];then
	mkdir /sys/fs/cgroup/yuri/${CGROUPNAME}
else
	echo "cgroup yuri/${CGROUPNAME} already exists"
fi

let totalmem=2*134217728 #256mb = (4*128mb)*50%

echo ${totalmem} > /sys/fs/cgroup/yuri/${CGROUPNAME}/memory.max
echo "set memory.max to"
cat /sys/fs/cgroup/yuri/${CGROUPNAME}/memory.max
exit 0
