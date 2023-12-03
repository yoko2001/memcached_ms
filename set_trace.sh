#! /bin/bash

#turn off trace first
echo 0 > /sys/kernel/debug/tracing/tracing_on

#check current tracer
echo "current_tracer are belows:"
echo > /sys/kernel/debug/tracing/set_event
echo "nop" > /sys/kernel/debug/tracing/current_tracer
#cat /sys/kernel/debug/tracing/current_tracer
#add lru_gen tracer
echo >> /sys/kernel/debug/tracing/set_ftrace_filter
# echo "*lru_gen*" >> /sys/kernel/debug/tracing/set_ftrace_filter
# echo "*mm_lru*" >> /sys/kernel/debug/tracing/set_ftrace_filter
# #echo "folio_add_lru" >> /sys/kernel/debug/tracing/set_ftrace_filter
# #echo "folio_add_lru_vma" >> /sys/kernel/debug/tracing/set_ftrace_notrace
# #echo "folio_*_gen" >> /sys/kernel/debug/tracing/set_ftrace_filter
# echo "split_huge_page_to_list" >> /sys/kernel/debug/tracing/set_ftrace_filter
# echo "mem_cgroup_swapin_charge_folio" >> /sys/kernel/debug/tracing/set_ftrace_filter
# echo "read_swap_cache_async" >> /sys/kernel/debug/tracing/set_ftrace_filter
# echo "swapin_readahead" >> /sys/kernel/debug/tracing/set_ftrace_filter
# echo "node_reclaim" >> /sys/kernel/debug/tracing/set_ftrace_filter
# echo "swap_cluster_readahead" >>  /sys/kernel/debug/tracing/set_ftrace_filter
# #echo "swap_readpage" >>  /sys/kernel/debug/tracing/set_ftrace_filter
# echo "try_to_inc_max_seq" >>  /sys/kernel/debug/tracing/set_ftrace_filter
# echo "folio_add_lru" >>  /sys/kernel/debug/tracing/set_ftrace_filter
# #echo "try_charge_memcg" >>  /sys/kernel/debug/tracing/set_ftrace_filter
# echo "try_to_free_mem_cgroup_pages" >>  /sys/kernel/debug/tracing/set_ftrace_filter
# echo "do_huge_pmd_anonymous_page" >>  /sys/kernel/debug/tracing/set_ftrace_filter
# echo "do_madvise" >>  /sys/kernel/debug/tracing/set_ftrace_filter


echo "add all lru_gen tracers below: "
echo > /sys/kernel/debug/tracing/set_ftrace_filter
cat /sys/kernel/debug/tracing/set_ftrace_filter
# echo "function" > /sys/kernel/debug/tracing/current_tracer
# echo $$ >> /sys/kernel/debug/tracing/set_ftrace_pid
echo 0 > /sys/kernel/debug/tracing/events/pagemap/enable
echo 0 > /sys/kernel/debug/tracing/events/lru_gen/enable
echo 0 > /sys/kernel/debug/tracing/events/swap/enable
echo 0 > /sys/kernel/debug/tracing/events/swap/get_swap_pages_noswap/enable
echo 0 > /sys/kernel/debug/tracing/events/swap/folio_add_to_swap/enable
echo 0 > /sys/kernel/debug/tracing/events/swap/swap_alloc_cluster/enable
echo 0 > /sys/kernel/debug/tracing/events/swap/scan_swap_map_slots/enable
echo 0 > /sys/kernel/debug/tracing/events/lru_gen/folio_delete_from_swap_cache/enable
echo 1 > /sys/kernel/debug/tracing/events/lru_gen/folio_ws_chg/enable
echo 1 > /sys/kernel/debug/tracing/events/lru_gen/folio_ws_chg_se/enable
echo 1 > /sys/kernel/debug/tracing/events/lru_gen/shadow_ext_transfer/enable
echo 0 > /sys/kernel/debug/tracing/events/lru_gen/mglru_sort_folio/enable
echo 0 > /sys/kernel/debug/tracing/events/lru_gen/walk_pte_range/enable
echo 0 > /sys/kernel/debug/tracing/events/lru_gen/mglru_isolate_folio/enable
echo 0 > /sys/kernel/debug/tracing/events/lru_gen/damon_folio_mark_accessed/enable
echo 0 > /sys/kernel/debug/tracing/events/swap/new_swap_ra_info/enable
echo 0 > /sys/kernel/debug/tracing/events/swap/do_swap_page/enable
echo 0 > /sys/kernel/debug/tracing/events/swap/do_anonymous_page/enable
echo 0 > /sys/kernel/debug/tracing/events/swap/swapin_force_wake_kswapd/enable
echo 0 > /sys/kernel/debug/tracing/events/swap/folio_inc_refs/enable
echo 0 > /sys/kernel/debug/tracing/events/swap/readahead_swap_readpage/enable
echo 0 > /sys/kernel/debug/tracing/events/swap/swapin_readahead_hit/enable
echo 0 > /sys/kernel/debug/tracing/events/swap/folio_add_lru/enable

#vmscan
echo 0 > /sys/kernel/debug/tracing/events/vmscan/enable
echo 0 > /sys/kernel/debug/tracing/events/vmscan/mm_shrink_slab_start/enable
echo 0 > /sys/kernel/debug/tracing/events/vmscan/mm_shrink_slab_end/enable
echo 0 > /sys/kernel/debug/tracing/events/vmscan/mm_vmscan_write_folio/enable
echo 0 > /sys/kernel/debug/tracing/events/vmscan/mm_vmscan_wakeup_kswapd/enable
echo 0 > /sys/kernel/debug/tracing/events/vmscan/mm_vmscan_kswapd_wake/enable
echo 0 > /sys/kernel/debug/tracing/events/vmscan/lru_gen_shrink_node/enable
echo 0 > /sys/kernel/debug/tracing/events/vmscan/evict_folios/enable
echo 0 > /sys/kernel/debug/tracing/events/vmscan/should_run_aging/enable
echo 0 > /sys/kernel/debug/tracing/events/vmscan/kswapd_shrink_node/enable

#kmem
echo 0 > /sys/kernel/debug/tracing/events/kmem/enable
