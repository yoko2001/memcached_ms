import libmc
import random
from threading import Thread
import time
import multiprocessing
import string

def generate_random_string(length):
    characters = string.ascii_letters + string.digits + string.punctuation
    random_string = ''.join(random.choice(characters) for _ in range(length))
    return random_string

def libmc_from_config(config):
    '''
    Sample config:
    {
        'SERVERS': ['localhost:11212 mc-server1',
                    'localhost:11213 mc-server2',
                    'localhost:11214 mc-server3',
                    'localhost:11215 mc-server4'],
        'HASH_FUNCTION': 'crc32',
        'PREFIX': '',
        'CONNECT_TIMEOUT': 10,
        'POLL_TIMEOUT': 300,
        'RETRY_TIMEOUT': 5,
        'COMPRESSION_THRESHOLD': 1024
    }
    '''

    _HASH_FN_MAPPING = {
        'md5': libmc.MC_HASH_MD5,
        'crc32': libmc.MC_HASH_CRC_32,
        'fnv1_32': libmc.MC_HASH_FNV1_32,
        'fnv1a_32': libmc.MC_HASH_FNV1A_32
    }
    servers = config['SERVERS']
    hash_fn = _HASH_FN_MAPPING[config.get('HASH_FUNCTION', 'md5')]
    prefix = config.get('PREFIX', '')
    connect_timeout = config.get('CONNECT_TIMEOUT', 10)
    poll_timeout = config.get('POLL_TIMEOUT', 300)
    retry_timeout = config.get('RETRY_TIMEOUT', 5)
    comp_threshold = config.get('COMPRESSION_THRESHOLD', 0)
    client = libmc.Client(
        servers,
        comp_threshold=comp_threshold,
        prefix=prefix,
        hash_fn=hash_fn,
    )
    client.config(libmc.MC_CONNECT_TIMEOUT, connect_timeout)
    client.config(libmc.MC_POLL_TIMEOUT, poll_timeout)
    client.config(libmc.MC_RETRY_TIMEOUT, retry_timeout)
    return client

class Rand_Load(object):
    def __init__(self, size_prob, prefix = None):
        super().__init__()
        self.size_prob = self.check_prob(size_prob)
        self.prefix = prefix
        if not self.prefix.endswith('_'):
            self.prefix += '_'
    #[{"size":100, "prob":0.1f},{"size":10, "prob":0.9f}]
    def check_prob(self, size_prob):
        probsum = 0.0
        for s_p in size_prob:
            prob = s_p["prob"]
            probsum += prob
        current_pro = 0.0
        for i in range(0, len(size_prob)):
            rawprob = size_prob[i]["prob"]
            size_prob[i]["prob"] = current_pro
            size_prob[i]["num"] = 0
            current_pro += rawprob / probsum
        return size_prob
    
    def next_name(self, s_p_n):
        return self.prefix + str(s_p_n["size"])+"_"+str(s_p_n["num"])
    
    def get_raw_load(self, size): #size in byte; aligned by byte*4
        assert(type(size) == int)
        return 'abcd' * int(size / 4)
        return generate_random_string((size * 4 + 3) / 4)
    def next_load(self):
        prob = random.random()
        n = len(self.size_prob)
        for i, s_p_n in  enumerate(self.size_prob[::-1]): #reversely get
            if prob > s_p_n['prob']: #match
                name = self.next_name(s_p_n)
                data = self.get_raw_load(s_p_n["size"])
                self.size_prob[len(self.size_prob)-i-1]["num"] += 1
                return (name, data)
        return None

class CliendWorker():
    def __init__(self, 
                 name, 
                 mc_config, 
                 load_config, 
                 access_methods, 
                 access_methods_rate , 
                 seed, 
                 num_load, 
                 num_total_size, 
                 get_distri = None
    ):
        # super().__init__(name=name)
        self.name=name
        self.mc_config = mc_config
        self.load_config = load_config
        self.load_generator = None
        self.access_methods = access_methods
        self.access_methods_rate = access_methods_rate
        self.mc = None
        self.seed = seed
        self.num_load = max(num_load, 500)
        self.keys = {} # "k1":len(data) , if value is 0 => k1 has been deleted
        self.freq_keys = []
        self.using_size = 0
        self.data_lost = 0
        self.num_total_size = num_total_size
        self.exp_time = 0 # 900s
        self.get_distri = get_distri #根据2-8定律输入，如果想要2-8定律，则输入0.2
    
    def get_access(self):
        return random.choices(self.access_methods, weights=self.access_methods_rate, k=1)[0]
    
    def get_rand_key(self):
        if self.get_distri:
            if self.get_distri >=1.0:
                return random.choice(list(self.keys.keys()))
            else:
                total_key_num = len(self.keys.keys())
                freq = random.random()
                if (freq < self.get_distri): #rare
                    return random.choice(list(self.keys.keys()))
                else: #0.8
                    return random.choice(self.freq_keys)
                
        else:
            return random.choice(list(self.keys.keys()))
    
    def run(self):
        print('%s running' % self.name)
        random.seed(self.seed)
        self.load_generator = Rand_Load(self.load_config, self.name) #get load gendrator   
        mc = libmc_from_config(self.mc_config) #get libmc client object

        #fill up
        while self.using_size < self.num_total_size:
            (key, data) = self.load_generator.next_load()
            self.using_size += (len(data)) #add this to remote
            mc.delete(key) #make sure its new
            mc.set(key, data)
            if not mc.get(key) == data:
                #retry 
                mc.set(key, data)
                if not mc.get(key) == data:
                    print(key, "init add fail")
                    self.keys[key] = -1
                else:
                    self.keys[key] = len(data)
            else:
                self.keys[key] = len(data)
            if self.keys[key] > 0 and random.random() < self.get_distri:
                self.freq_keys.append(key) # key has higher frequencey
        print("worker {} total use size {} byte, {} keys init".format(self.name, self.using_size, len(self.keys.keys())))
        #now its filled up
        for i in range(0, self.num_load):
            access = self.get_access()
            choosed_key = self.get_rand_key()
            if access == "set":
                data = mc.get(choosed_key)
                if data == None:  #lost
                    (key, data) = self.load_generator.next_load()
                    mc.set(choosed_key, data) #reset one
                    if not mc.get(choosed_key) == data:
                        #recheck
                        mc.set(choosed_key, data) #reset one
                        if not mc.get(choosed_key) == data:
                            print(choosed_key, "set check fail")
                            self.keys[choosed_key] = -1
                        else:
                            self.keys[choosed_key] = len(data)  #update key
                    else:
                        self.keys[choosed_key] = len(data)  #update key
                elif not len(data) == self.keys[choosed_key]:
                    if not self.keys[choosed_key] == -1:
                        print("err key %s", choosed_key)
                    else:
                        self.keys[choosed_key] = len(data)  #update key
                else: #good case
                    shuffled_data = list(data)
                    random.shuffle(shuffled_data)
                    shuffled_data = ''.join(shuffled_data)
                    mc.set(choosed_key, shuffled_data)
                    if not mc.get(choosed_key) == shuffled_data:
                        #retry
                        mc.set(choosed_key, shuffled_data)
                        if not mc.get(choosed_key) == shuffled_data:
                            print(choosed_key, "shuffled_data set fail")
                            self.keys[choosed_key] = -1
                        else:
                            self.keys[choosed_key] = len(shuffled_data)
            elif access == "get":
                data = mc.get(choosed_key)
                if data == None:  #lost                
                    print(choosed_key, "get lost")
                    self.keys[choosed_key] = -1
                    self.data_lost += 1
                elif not len(data) == self.keys[choosed_key]:
                    if not self.keys[choosed_key] == -1:
                        print("err key %s", choosed_key)
                    else:
                        self.keys[choosed_key] = len(data)  #update key
            else:
                print("unkown access %s", access)
        print('%s finished' % self.name)
        print("data lost ", self.data_lost)
def task(i, client):
    start_time = time.time()
    client.run()
    end_time = time.time()
    print("process %d : time %d", i, end_time- start_time)

#16个进程 每个进程存30mb at most
#理论可存储128*4
if __name__ == "__main__":
    num_worker = 16
    seeds_list = list(map(lambda x: x, range(1, num_worker + 1)))
    print(seeds_list)
    config = {
        'SERVERS': ['localhost:11212 memcached_server1',
                    'localhost:11213 memcached_server2',
                    'localhost:11214 memcached_server3',
                    'localhost:11215 memcached_server4'],
        'HASH_FUNCTION': 'crc32',
        'PREFIX': '',
        'CONNECT_TIMEOUT': 100,
        'POLL_TIMEOUT': 3000,
        'RETRY_TIMEOUT': 5,
        'COMPRESSION_THRESHOLD': 0
    }

    load_config = [
        # {"size":96, "prob": 0.4},
        # {"size":384, "prob": 0.3},
        {"size":768, "prob": 0.2},
        {"size":1536, "prob": 0.1},
    ]
    
    access_class = ["set", "get"]
    access_class_rate = [0.2, 0.8] 

    clients = []
    for w in range(1, num_worker+1):
        client = CliendWorker(
            "worker_"+str(w),
            config,
            load_config,
            access_class,
            access_class_rate,
            seeds_list[w-1],
            num_load=160000,
            num_total_size = 16777216*2,#15728640*2,#15mb 
            get_distri=1.1#0.3
        )
        clients.append(client)
  
    pool = multiprocessing.Pool()
      
    #run
    print("started all %d workers ", len(clients))
    start_time = time.time()
    for i, client in enumerate(clients):
        pool.apply_async(task, args=(i, client))

    pool.close()
    pool.join()

    end_time = time.time()
    execution_time = end_time - start_time
    print("finish, used ", execution_time)
    
    exit(0)
