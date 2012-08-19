
import socket
import time
import sys

try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
except socket.error, msg:
        print msg
        sys.exit(1)
try:
        s.connect(("127.0.0.1", 8046))
except socket.error, msg:
        s.close()
        print msg
        sys.exit(1)

s.send("set test1 4\r\n")
s.send("test")
data = s.recv(1024)
print data

s.send("get test1\r\n")
data = s.recv(1024)
print data

s.send("delete test1\r\n")
data = s.recv(1024)
print data
