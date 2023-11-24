#!/bin/bash

modprobe brd rd_nr=1 rd_size=$((512 * 1024))
swapoff /dev/ram0

#swap on
mkswap /dev/ram0
#setting the max priority
swapon -p 1005 /dev/ram0

