import re 
import os
import sys

#CGROUP
cgruop_name='memcached_server'
cgroup_prefix='yuri'
cgroup_root=os.path.abspath('/sys/fs/cgroup')

#LOGS
log_root=os.path.abspath('/var/log')
server_names = [
    'memcached_server1',
    'memcached_server2',
    'memcached_server3',
    'memcached_server4',
]

#we have to be sure
def check_memory_file_single(file_path):
    #read single line from file
    with open(file_path) as f:
        result = f.readlines()
        if (len(result) > 1):
            raise SystemError("got more than one line from {}".format(file_path))
        else:
            return int(result[0])
        
def check_memory_stat(cgroup):
    focus_stats_keywords = [
        'attempt_assign'
        'assign_',
        'swap_out',
        'swap_in_from',
        'workingset_refault',
    ]
    file_name = 'memory.stat'
    file_path = os.path.join(cgroup, file_name)
    if not os.path.exists(file_path):
        raise FileNotFoundError("{} doesn't exists".format(file_path))
    result = []
    with open(file_path) as f:
        while True:
            fileline = f.readline()
            if not fileline: # endoffile
                return result
            for prefix_pat in focus_stats_keywords: # check patterns
                if fileline.startswith(prefix_pat):
                    ret = re.match("{}[a-zA-Z_]+ [0-9]+\n".format(prefix_pat), fileline)
                    if ret:
                        ret_2 = ret.group()[:-1].split(" ")
                        if not len(ret_2) == 2:
                            return IndexError("line should have one name and one value : {}".format(ret))
                        result.append(str("{0:<30}:{1:<12}".format(ret_2[0], ret_2[1])))                    
                    break

def check_memory_peak(cgroup):
    file_name = 'memory.peak'
    file_path = os.path.join(cgroup, file_name)
    if not os.path.exists(file_path): # safe check
        raise FileNotFoundError("{} doesn't exists".format(file_path))
    return check_memory_file_single(file_path)

def check_memory_max(cgroup):
    file_name = 'memory.max'
    file_path = os.path.join(cgroup, file_name)
    if not os.path.exists(file_path):
        raise FileNotFoundError("{} doesn't exists".format(file_path))
    return check_memory_file_single(file_path)

def check_cgroup(cgroup):
    cg_checks = {
        'memory.peak': check_memory_peak, 
        'memory.max' : check_memory_max,
        'memory.stat': check_memory_stat, 
    }
    for check in cg_checks.keys():
        result = cg_checks[check](cgroup)
        if result:
            if type(result) == int:
                print("{0:<12}:{1:<12} ({2:<6.2f}MB)".format(check, result, result / (1024*1024)))
            elif type(result) == list:
                for res_line in result:
                    print(res_line)
    return

def get_client_id(record):
    pattern1= "<[0-9]+ "
    pattern2= ">[0-9]+ "
    res = re.search(pattern1, record)
    if res:
        return res.group()[1:-1]
    res = re.search(pattern2, record)
    if res:
        return res.group()[1:-1]

def check_log(filename, filepath):
    set_pattern = '<[0-9]+ set [a-zA-Z0-9_]+ [0-9]+ [0-9]+ [0-9]+' # set key flags exptime bytes [noreply] 
    delete_pattern = '<[0-9]+ delete [a-zA-Z0-9_]+' 
    get_pattern = '<[0-9]+ get [a-zA-Z0-9_]+' 
    connection_pattern = '<[0-9]+ new auto-negotiating client connection'
    re_stored_pattern = '>[0-9]+ STORED'
    re_deleted_pattern = '>[0-9]+ DELETED'
    re_end_pattern = '>[0-9]+ END'
    re_not_found_pattern = '>[0-9]+ NOT_FOUND'
    re_sending_pattern = '>[0-9]+ sending key [a-zA-Z0-9_]+'

    if not os.path.exists(filepath): # safe check
        raise FileNotFoundError("{} doesn't exists".format(filepath))
    #read single line from file
    result = {
        'stat' : {
            'total_key_num':0,
            'total_item_size':0,
            'clients' : {}
        },
        'records' : {}
    }
    pareless = {}
    last_line = {}
    with open(filepath) as f:
        lines = f.readlines()
        for line in lines:
# try if it is set
            ret = re.search(set_pattern, line)
            if (ret):
                record = ret.group()
                client_id = get_client_id(record)
                rec_set = record.split('<' + client_id + ' ')[-1]
                pareless[client_id]['set'].append(rec_set)
                assert(rec_set.startswith('set'))
                last_line[client_id] = 'set'
                continue
#try if it is delete
            ret = re.search(delete_pattern, line)
            if (ret):
                record = ret.group()
                client_id = get_client_id(record)
                rec_delete = record.split('<' + client_id+ ' ')[-1]
                pareless[client_id]['delete'].append(rec_delete)
                assert(rec_delete.startswith('delete'))
                last_line[client_id] = 'delete'
                continue            
#try if it is get
            ret = re.search(get_pattern, line)
            if (ret):
                record = ret.group()
                client_id = get_client_id(record)
                rec_get = record.split('<' + client_id+ ' ')[-1]
                pareless[client_id]['get'].append(rec_get)
                assert(rec_get.startswith('get'))
                last_line[client_id] = 'get'
                continue   
#try if it is reply stored
            ret = re.search(re_stored_pattern, line)
            if (ret):
                record = ret.group()
                client_id = get_client_id(record)
                assert(last_line[client_id] == 'set')
                if len(pareless[client_id]['set']) == 0:
                    raise IndexError("pareless not pared")
                request = pareless[client_id]['set'].pop()
                #success store
                key = request.split(' ')[1]
                size = request.split(' ')[4]
                result['stat']['total_key_num'] += 1
                result['stat']['total_item_size'] += int(size)
                result['stat']['clients'][client_id]['num_set'] += 1
                result['stat']['clients'][client_id]['total_data_size'] += int(size)
                result['stat']['clients'][client_id]['live_data_size'] += int(size)
                result['records'][key] = {'status':'live'}

#try if it is reply deleted
            ret = re.search(re_deleted_pattern, line)
            if (ret):
                record = ret.group()
                client_id = get_client_id(record)
                assert(last_line[client_id] == 'delete')
                if len(pareless[client_id]['delete']) == 0:
                    raise IndexError("pareless not pared")
                request = pareless[client_id]['delete'].pop()

                continue
#try if it is reply end
            ret = re.search(re_end_pattern, line)
            if (ret):
                continue
#try if it is reply not found
            ret = re.search(re_not_found_pattern, line)
            if (ret):
                continue
#try if it is reply seeding key
            ret = re.search(re_sending_pattern, line)
            if (ret):
                continue
#try if it is connection establish
            ret = re.search(connection_pattern, line)
            if (ret):
                cliend_id = ret.group()[1:].split(" new auto-negotiating client connection")[0]
                result['stat']['clients'][cliend_id] = {
                    'num_get': 0,
                    'fail_get' : 0,
                    'num_set': 0,
                    'fail_set' : 0,
                    'num_delete': 0,
                    'fail_delte' : 0,
                    'total_data_size' : 0,
                    'live_data_size' : 0,
                }
                pareless[cliend_id] = {
                    'set' : [],
                    'get' : [],
                    'delete' : [],
                }
#known record
            # print(line)
    print(result['stat'])
    return 
def check_logs(log_files):
    for filename in log_files.keys():
        ret = check_log(filename, log_files[filename])
        print(ret)
if __name__ == "__main__":
    #check cgroup path
    cgroup_prefix_root = os.path.join(cgroup_root, cgroup_prefix)
    if not os.path.exists(cgroup_prefix_root):
        raise FileNotFoundError("{} doesn't exists".format(cgroup_prefix_root))
    cgroup_name_root = os.path.join(cgroup_prefix_root, cgruop_name)
    if not os.path.exists(cgroup_name_root):
        raise FileNotFoundError("{} doesn't exists".format(cgroup_name_root))
    check_cgroup(cgroup_name_root)
    log_files = {}
    #check log path
    for server_name in server_names:
        server_file = server_name + ".log"
        server_log_path = os.path.join(log_root, server_file)
        if not os.path.exists(server_log_path):
            raise FileNotFoundError("{} doesn't exists".format(server_log_path))
        log_files[server_name] = server_log_path
    check_logs(log_files)