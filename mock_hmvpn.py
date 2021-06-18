# a = str(input("Enter user name: "))
# b = str(input("Enter password: "))
# code

connected = """Detected distro: Ubuntu 18.04.1 LTS

Calling OpenVPN as process...
Connecting to " USA, Utah, Salt Lake City  ut.us.hma.rocks " (udp/553)
Enter CTRL-C to terminate connection
Waiting for connection to complete...
Option 'explicit-exit-notify' in [PUSH-OPTIONS]:5 is ignored by previous <connection> blocks
Option 'explicit-exit-notify' in [PUSH-OPTIONS]:13 is ignored by previous <connection> blocks
do_ifconfig, tt->did_ifconfig_ipv6_setup=1
Your IP is
199.189.106.241
Connected to "  USA, Utah, Salt Lake City  ut.us.hma.rocks " (udp/553)
"""

print(connected)
input()
killed = "^CStopping HMA VPN"
