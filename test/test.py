
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

s.send("set test1 0 4\r\n")
s.send("test")
data = s.recv(1024)
print data

s.send("get test1\r\n")
data = s.recv(1024)
print data

s.send("delete test1\r\n")
data = s.recv(1024)
print data

# exptime
s.send("set test2 3s 4\r\n")
s.send("test")
data = s.recv(1024)
print data

s.send("get test2\r\n")
data = s.recv(1024)
print data

time.sleep(4)

s.send("get test2\r\n")
data = s.recv(1024)
print data

#batch get 

s.send("set test5 0s 4\r\n")
s.send("test")
data = s.recv(1024)
print data


s.send("set test6 0s 4\r\n")
s.send("test")
data = s.recv(1024)
print data

s.send("get test5 test6\r\n")
data = s.recv(1024)
print data

# replace 
s.send("replace test7 0s 4\r\n")
s.send("test")
data = s.recv(1024)
print data

s.send("set test7 0s 4\r\n")
s.send("test")
data = s.recv(1024)
print data

s.send("replace test7 0s 5\r\n")
s.send("test7")
data = s.recv(1024)
print data

s.send("get test7\r\n")
data = s.recv(1024)
print data

#add

s.send("add test8 0s 5\r\n")
s.send("test8")
data = s.recv(1024)
print data

s.send("add test8 0s 5\r\n")
s.send("test8")
data = s.recv(1024)
print data

s.send("delete test8\r\n")
data = s.recv(1024)
print data

s.send("add test8 0s 5\r\n")
s.send("test8")
data = s.recv(1024)
print data


s.send("get test8\r\n")
data = s.recv(1024)
print data
