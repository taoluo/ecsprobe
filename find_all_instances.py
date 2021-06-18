import os
import shlex, subprocess
from functools import partial
from pprint import pprint
class ConnectionFail(Exception):
    pass

missing_pop_set = {
'hhn',
'del',
'waw',
'yyz',
'mel',
'mrn',
'lhr',
'las',
'cgk',
'icn',
'slc',
'kix'
}

instance_list = [("nord", "SLC")]  # , ("nord", "LA")]
vpn_server_file = "./data/VPN/HMA_servers.txt"
VPN_provider = os.path.basename(vpn_server_file).split(".")[0]
with open(vpn_server_file) as f:
    instance_list = [l.strip() for l in f if len(l.strip()) != 0]
MOCK = False
do_analyze_log = False
sudo_prefix = "echo '123' | sudo -S "

import re
import ipaddress
import logging
import subprocess
import time

# proc = subprocess.Popen(
#     ["python", "mock_hmvpn.py"],
#     stdin=subprocess.PIPE,
#     stdout=subprocess.PIPE,
#     stderr=subprocess.PIPE,
#     shell=False,
# )
# out, _ = proc.communicate(bytes("tluo7171\nabcdefg,.\n", "utf-8"))
# print(out.decode("utf-8"))
# import signal


class HMA_VPN:
    def __init__(self, server):
        subprocess.check_output(
            "echo tluo7171 > /tmp/vpnlogin && echo abcdefg,. >> /tmp/vpnlogin",
            shell=True,stderr=subprocess.DEVNULL
        )
        self.proc = None
        self.server = server

    # @setter
    # def server(s):
    #     self.server

    def __enter__(self):
        # assert self.server != None

        if MOCK:
            start_vpn_cmd = ["python", "mock_hmvpn.py", "-p", "udp", self.server]
        else:
            # "echo '123' | sudo -S ./VPN/hma-vpn.sh -c /tmp/vpnlogin -d de.hma.rocks"
            start_vpn_cmd =  sudo_prefix + f"./VPN/hma-vpn.sh -c /tmp/vpnlogin -d {self.server}"

        for i in range(33):
            start_proc = subprocess.run(
                start_vpn_cmd,
                # stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                # stderr=subprocess.DEVNULL,
                shell=True,
                # preexec_fn=os.setsid,
            )
            for i in range(150):
                time.sleep(0.2)

                if MOCK:
                    check_status_cmd = "echo Connected"
                else:
                    check_status_cmd = sudo_prefix + "./VPN/hma-vpn.sh -s;exit 0"
                # try:
                # print("\n\n")
                a = subprocess.check_output(check_status_cmd,stderr=subprocess.STDOUT,shell=True) # stderr=subprocess.DEVNULL
                # print(a)
                if a.decode("utf8").startswith("Connected"):
                    print(f"connected to {self.server}")
                    break
            else:
                continue
            break
            # except Exception as e:
                # print(a)
                # print(e)
        else:
            raise ConnectionFail(f"fail to connect to {self.server} after retry")
        # print("pid %s" % self.proc.pid)
        # out, _ = self.proc.communicate(bytes("tluo7171\nabcdefg,.\n", "utf-8"))
        # print(out)

    def __exit__(self, a, b, c):
        # os.killpg(
        #     os.getpgid(self.proc.pid), signal.SIGTERM
        # )  # Send the signal to all the process groups
        # self.proc.terminate()
        # output = subprocess.check_output("echo x", shell=True)
        if MOCK:
            kill_deamon_cmd = f"echo xxx"
        else:
            kill_deamon_cmd = sudo_prefix + "./VPN/hma-vpn.sh -x"
        output = subprocess.check_output(kill_deamon_cmd, shell=True,stderr=subprocess.DEVNULL,)
        print(f"disconnected from {self.server}")
        # self.proc.wait()


# https://stackoverflow.com/questions/11264005/using-a-regex-to-match-ip-addresses-in-python/11264379
def valid_ip(address):
    try:
        ipaddress.ip_address(address)
        return True
    except:
        return False


DNS_addr='8.8.8.8'
# DNS_addr = "119.29.29.29"
iter_n = 20
myaddr_gg = "o-o.myaddr.l.google.com"
loop_dig_cmd = f"for i in `seq 1 {iter_n}`; do dig @{DNS_addr}  {myaddr_gg}  TXT +short; done"
myip_cmd = f"dig @ns1.google.com o-o.myaddr.l.google.com  TXT +short"

# args = shlex.split(dig_cmd)
# myip_cmd = f"dig @ns1.google.com o-o.myaddr.l.google.com  TXT +short"

num_myip_retries = 10
my_ip_addr: str


def retry(call, times):
    def wrapper(*args, **kwargs):
        for i in range(times):
            try:
                # myip_data = subprocess.check_output(myip_cmd, shell=True)
                # my_ip_addr = myip_data.decode('utf8').strip()[1:-1]
                return call(*args, **kwargs)
            except Exception as e:
                lgr.warning(f"fail to exec cmd {args[0]}, {i}th time")
                lgr.warning(e)
        else:
            raise Exception(f"aborting, fail to exec cmd {args[0]}")

    return wrapper


@partial(retry, times=10)
def run_cmd(cmd):
    data = subprocess.check_output(cmd, shell=True)
    return data.decode("utf8")


# for i in num_myip_retries:
#     try:
#         myip_data = subprocess.check_output(myip_cmd, shell=True)
#         my_ip_addr = myip_data.decode('utf8').strip()[1:-1]
#     except Exception as e:
#         warnings.warn(f"{i}: fail to get my ip addr")
#         warnings.warn(e)
# else:
#     raise Exception("cannot get own ip addr aborting")
import logging

# logging.basicConfig(format='%(asctime)s,%(name)s,%(levelname)s,%(message)s', level=logging.DEBUG)

# logger = logging.getLogger(name='hitted_PoP_IP')
# create logger
lgr = logging.getLogger(name="hitted_PoP_IP")
lgr.setLevel(logging.DEBUG)  # log all escalated at and above DEBUG
# add a file handler
from datetime import datetime
vpn_log_file = "VPN_PoP_IP_%s.csv" % datetime.strftime(datetime.now(),"%m-%d-%H-%M-%S")
fh = logging.FileHandler(vpn_log_file,mode='w')
fh.setLevel(logging.DEBUG)  # ensure all messages are logged to file

# create a formatter and set the formatter for the handler.
frmt = logging.Formatter("%(asctime)s,%(name)s,%(levelname)s,%(message)s")
fh.setFormatter(frmt)

# add the Handler to the logger
lgr.addHandler(fh)

instance_pop_ip_mapping ={}
for instance in instance_list:
    my_ip_addr = ""
    instance_pop_ip_mapping[instance] = set()
    # VPN_provider = 'nord'
    # instance = 'SLC'
    # vpn.server = 'uta.us.hm'
    # print(instance)
    try:
        with HMA_VPN(instance) as vpn:

            # run_cmd(switch_VPN_cmd(VPN_provider, instance))
            my_ip_addr = run_cmd(myip_cmd).strip()[1:-1]
            pop_ip_addr_data = run_cmd(loop_dig_cmd).strip()

            # data = subprocess.check_output(dig_cmd, shell=True)
            for res in pop_ip_addr_data.split("\n"):
                pop_ip = res[1:-1]
                if len(res) > 2 and valid_ip(pop_ip):  # at least contains " "
                    lgr.info([my_ip_addr, VPN_provider, instance, pop_ip],)
                    instance_pop_ip_mapping[instance].add(pop_ip)
                else:
                    
                    if not res.startswith('"edns0'):
                        print(f"dig TXT response {res}")

    except ConnectionFail as e:
        lgr.warning(f"cannot connect to {VPN_provider}- {instance}")
        continue
    except Exception as e:
        lgr.warning(f"{VPN_provider}-{instance} fail to dig : {e}")
        continue
                # continue
            # else:
            #     # pass
            #     lgr.warning((my_ip_addr, VPN_provider, instance, res),)

print(instance_pop_ip_mapping)

import ipaddress
if do_analyze_log:
    instance_pop_ip_mapping = set()
    
    pop_ip_set = set()
    pop_addr_pat = re.compile("'[a-f0-9\.:]+']$")
    with open(vpn_log_file, "r") as f:
        for line in f:
            line_fld = line.split(',')
            instance = line_fld[-2][2:-1]
            pop_ip = line_fld[-1][2:-2]
            instance_pop_ip_mapping[instance].add(pop_ip)
            # print(line)
            # print(re.findall(pop_addr_pat, line)[0][1:-2])
            # pop_ip_set.add(re.findall(pop_addr_pat, line)[0][1:-2])
        # print(len(pop_ip_set))
        # print(pop_ip_set)
print("-----------------")
print(instance_pop_ip_mapping)


def _is_ip_v4_or_v6(ip_addr):
    if "." in ip_addr:
        return "v4"
    elif ":" in ip_addr:
        return "v6"
    else:
        return None


v4prefix_pop_mapping = {}
v6prefix_pop_mapping = {}
with open("./data/GPDNS_backend_ipprefix.txt", "r") as f:
    for line in f:
        l = line.strip().split(" ")
        if _is_ip_v4_or_v6(l[0]) == "v4":
            v4prefix_pop_mapping[ipaddress.IPv4Network(l[0])] = l[1]
        elif _is_ip_v4_or_v6(l[0]) == "v6":
            v6prefix_pop_mapping[ipaddress.IPv6Network(l[0])] = l[1]

# print(v4prefix_pop_mapping)
# print(v6prefix_pop_mapping)
pop_set_all = set()
instance_pop_loc_mapping = {}
for instance, popip_set in instance_pop_ip_mapping.items():
    instance_pop_loc_mapping[instance] = set()
    for popip in popip_set:
        if _is_ip_v4_or_v6(popip) == "v4":
            search_dict = v4prefix_pop_mapping
        else:
            assert _is_ip_v4_or_v6(popip) == "v6"
            search_dict = v6prefix_pop_mapping

        for prefix, popname in search_dict.items():
            if ipaddress.ip_address(popip) in prefix:
                print(f"{popip} is from {popname}")
                pop_set_all.add(popname)
                instance_pop_loc_mapping[instance].add(popname)
                break
        else:
            print(f"{popip}, pop prefix not matched")

pprint(instance_pop_loc_mapping)
print(f"NEW found pops {missing_pop_set.intersection(pop_set_all)}" )
print(f"MISSING pops {missing_pop_set - pop_set_all}" )
