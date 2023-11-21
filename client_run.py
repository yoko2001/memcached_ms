import libmc


def libmc_from_config(config):
    '''
    Sample config:
    {
        'SERVERS': ['localhost:11211 mc-server111',
                    'localhost:11212 mc-server112'],
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