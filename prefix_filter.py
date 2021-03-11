# from ripe.atlas.sagan import Result
from ripe.atlas.sagan.traceroute import TracerouteResult, Hop, Packet

# result = Result.get(
#     '{"af":4,"dst_addr":"121.244.76.25","dst_name":"121.244.76.25","endtime":1340329208,"from":"107.3.81.49","fw":4460,"msm_id":1000157,"paris_id":2,"prb_id":190,"proto":"UDP","result":[{"hop":1,"result":[{"from":"192.168.1.1","rtt":2.7829999999999999,"size":96,"ttl":64},{"from":"192.168.1.1","rtt":2.4500000000000002,"size":96,"ttl":64},{"from":"192.168.1.1","rtt":2.3210000000000002,"size":96,"ttl":64}]},{"hop":2,"result":[{"x":"*"},{"x":"*"},{"x":"*"}]},{"hop":3,"result":[{"x":"*"},{"x":"*"},{"x":"*"}]},{"hop":4,"result":[{"x":"*"},{"x":"*"},{"x":"*"}]},{"hop":5,"result":[{"x":"*"},{"x":"*"},{"x":"*"}]},{"hop":6,"result":[{"x":"*"},{"x":"*"},{"x":"*"}]},{"hop":255,"result":[{"x":"*"},{"x":"*"},{"x":"*"}]}],"size":40,"src_addr":"192.168.1.107","timestamp":1340329190,"type":"traceroute"}')
#
# # hop is not None and is ipv4
import ipaddress as ip
import os
import multiprocessing as mp
from io import SEEK_END
import re
import pandas as pd
# from pandarallel import pandarallel
# pandarallel.initialize()
IS_DEBUG = True
IS_multicore = True
from pprint import pprint
# from cyberpandas import IPArray,IPType, to_ipaddress, IPNetworkType, IPNetworkArray, to_ipnetwork
import logging
import time
logging.basicConfig(level=logging.ERROR)


def IP_to_int(ip_address):
    return int("".join( ("%02x" % int(x) for x in ip_address.split(".")) ), 16)

def _parse_prefix2asn(path=None):
    sep = re.compile(',|_')
    as_prefix = pd.read_csv(path, sep='\t')
    as_prefix['subnet'] = as_prefix['IP'] + '/' + as_prefix['mask'].astype(str)
    # as_prefix['subnet'] = as_prefix['subnet'].astype(IPNetworkType())  # too slow
    as_prefix['ASN'] = as_prefix['ASN'].apply(lambda x: re.split(',|_',x))
    as_prefix = as_prefix.explode(column='ASN',ignore_index=True)
    as_prefix['ASN'] = as_prefix['ASN'].astype(int)
    network_addr = as_prefix.IP.map(IP_to_int ) # lambda y: int("".join(["%02x" % int(x) for x in y.split(".")]), 16))
    netmask = as_prefix['mask'].map(lambda x: (0xffffffff << (32 - x)) & 0xffffffff)
    masked_network_addr = (network_addr & netmask)
    as_prefix['netmask'] = netmask
    as_prefix['masked_network_addr'] = masked_network_addr
    return as_prefix


def _parse_population_asn(path=None):
    as_population = pd.read_csv(path)
    as_population_total = as_population.groupby('ASN')['Users (est.)'].sum().reset_index()
    as_population_total['ASN'] = as_population_total['ASN'].apply(lambda x: int(x[2:]) )
    return as_population_total

# if IS_DEBUG:
prefix_as = _parse_prefix2asn('./data/routeviews-rv2-20210303-2000.pfx2as.tsv')
# else:
#     prefix_as = _parse_prefix2asn('./data/debug.pfx2as.tsv')

as_population = _parse_population_asn(path='./data/APNIC_ASN_population.csv')



populated_as_prefix = pd.merge(left=as_population, right=prefix_as, on='ASN', how='inner' )
asn_count = populated_as_prefix.groupby('subnet')['ASN'].count()
prefix_as_count = asn_count.reset_index()
prefix_as_count.columns = ['subnet', 'ASN_count']
print('Prefix with more than 1 asn')
print(prefix_as_count[prefix_as_count['ASN_count'] > 1].head(20))


start = time.time()

# intaddr = route_df.prefix.str.split(‘/’).str[0] \
#                   .map(lambda y: int(‘’.join([‘%02x’ % int(x)
#                        for x in y.split(‘.’)]), 16))
# netmask = route_df.prefixlen \
#           .map(lambda x: (0xffffffff << (32 — x)) & 0xffffffff)
# match = (dstip._ip & netmask) == (intaddr & netmask)
# result_df = route_df.loc[match.loc[match].index] \
#                     .sort_values(‘prefixlen’, ascending=False) \
#                     .drop_duplicates([‘vrf’])

# populated_as_prefix['IP'] = to_ipaddress(populated_as_prefix['IP'])
# prefix_as_count['subnet'] = to_ipnetwork(prefix_as_count['subnet'])

end = time.time()
print('subnet converstion done, takes %i Sec ' % (end - start))
#
# Worker process, return a boolean of prefix
#
def tag_hop_prefix(fname, prefix_to_check,  start, stop):
    """
    Process file[start:stop]

    start and stop both point to first char of a line (or EOF)
    """
    # a = 0
    # b = 0
    # c = 0
    is_hop_prefix = pd.Series([False] * prefix_to_check.shape[0])
    # is_hop_prefix = pd.Series([False] * len(prefix_to_check))
    # with open('./data/fa4.txt', 'r') as f:
    #     for line in f:
    #         result = TracerouteResult.get(line)
    #         for hop in result.hops:  # todo exclude last 2 hop ip addr
    #             if len(hop.packets) != 0 and hop.packets[0].origin is not None:
    #                 # check the pandas table.
    #                 populated_as_prefix['is_hop_prefix'] |= populated_as_prefix['subnet'].ipnet.subnet_of(
    #                     hop.packets[0].origin)

    with open(fname, 'r', newline='') as inf:
        # jump to start position
        pos = start
        inf.seek(pos)

        for line in inf:
            # value = int(line.split(4)[3])

            # *** START EDIT HERE ***
            #
            result = TracerouteResult.get(line)
            if result.total_hops >= 5:
                for hop in result.hops[2:-2]:  # todo exclude last 2 hop ip addr ???
                    if len(hop.packets) != 0 and hop.packets[0].origin is not None:
                        # check the pandas table.
                # update a, b, c based on value ## fixme

                        is_hop_prefix |= prefix_to_check['masked_network_addr'] == (prefix_to_check['mask'] & IP_to_int(hop.packets[0].origin))

            #
            # *** END EDIT HERE ***

            pos += len(line)
            if pos >= stop:
                break

    return is_hop_prefix

def main(num_workers, fname):
    num_tasks = num_workers * 10
    # for each input file
    # for fname in sam_files:
    print("Dividing {}".format(fname))
    # decide how to divide up the file
    with open(fname) as inf:
        # get file length
        inf.seek(0, SEEK_END)
        f_len = inf.tell()
        # find break-points
        starts = [0]
        for n in range(1, num_tasks):
            # jump to approximate break-point
            inf.seek(n * f_len // num_tasks)
            # find start of next full line
            inf.readline()
            # store offset
            starts.append(inf.tell())

    starttime = time.time()
    if IS_multicore:
        print("{} workers".format(num_workers))
        pool = mp.Pool(processes=num_workers)

        # do it!
        stops = starts[1:] + [f_len]
        start_stops =  zip(starts, stops)
        print("Solving {}".format(fname))

        results = [pool.apply_async(tag_hop_prefix, args=(fname, populated_as_prefix[['masked_network_addr','mask']] , start, stop)) for start,stop in start_stops]
        pool.close()
        pool.join()
        endtime = time.time()
        print("pool execution done, takes %.2f Min" % ((endtime-starttime)/ 60))
        # collect results
        is_hop_prefix = None
        for res in results:
            if is_hop_prefix is not None:
                is_hop_prefix |= res.get()
            else:
                is_hop_prefix = res.get()
    else:# single core
        is_hop_prefix = tag_hop_prefix( fname, populated_as_prefix[['masked_network_addr','mask']] , 0, f_len)

    prefix_as_count['is_hop_prefix'] = is_hop_prefix
    prefix_as_count.to_csv('./data/populated_prefix_hop.csv',index=False)

if __name__ == '__main__':
    if IS_DEBUG:
        main(os.cpu_count(),'./data/debug_fa4.txt')
    else:
        main(os.cpu_count(), './data/fa4.txt')

    print('finished')








