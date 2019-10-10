import socket
import time
import os


my_ip_address = socket.gethostbyname(socket.gethostname())

while True:
    free_m = os.popen('free -m').readlines()[1].split()[3]
    print("Server IP: %s, Free Memory: %s MB, Check Time: %s" %(my_ip_address, free_m, time.strftime("%H:%M:%S", time.localtime())))
    time.sleep(15)
