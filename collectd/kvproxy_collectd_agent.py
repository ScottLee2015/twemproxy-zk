#!/usr/bin/python  
#
# Copyright (c) 2016, HoneycombData, Inc. All rights reserved 
#
# authour Scott Lee   <scottlee@hcdatainc.com>

"""
   hmem cache monitoring agent

   an api that handles 
      1. WRITE command from hmem libs
      2. READ command from CollectD plugin

    argument: ip address & port# to the socket

    WRITE/READ command spec 
      input : agent_w/agent_r 
      output: monitoring stat in json format

    example:
        from server side:
          $ python hmem_agent('localhost', 23000)

        from client side:
          $ telnet localhost 23000
            READ
"""

import socket
import json
import pprint
import time
from collections import defaultdict
#import dictionary


# definition
backlog      = 10
cmd_length   = 7 
WRITE_CMD    = 'agent_w'
READ_CMD     = 'agent_r'
wbuffer_size = 10480
rbuffer_size = 10480


buf  = {} 


def get_max(v1, v2):
    """
    Return max of the two values
    """
    return v1 if v1 > v2 else v2

#temp = {}
# subroutines
def open_socket(ip, port):
    # open a socket, add a listner and wait for a client
    print '  ip is ' + ip + '.'
    srvsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srvsock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srvsock.bind((ip, port))
    print '  connected to ' + str(ip) + ':' + str(port)
    srvsock.listen(backlog)  # backlog up to 5

    # return server socket
    return srvsock


def get_device_id(data_json):
    d_id = data_json['proxy_instance']

    return d_id


def store_object(d_id, t_stamp, dataj):
    #TODO LRU
    global buf
    buf[d_id] = dataj
    #print '.# of clients: ' + str(len(buf))
 

def retrieve_data():
    global buf
    return json.dumps(buf)


def write_cmd(client_socket):
    """
      "kvstore_status": {
         "storage_path" : "/data/NVME0/d2/store-1",
    """
    data = client_socket.recv(wbuffer_size)
    print data
    # convert from string format to json format
    dataj = json.loads(data)
    #print dataj
    # get device id
    d_id = get_device_id(dataj)
    # get time stamp
    t_stamp = time.time()
    # now store to object array
    store_object(d_id, t_stamp, dataj)
    #store_object(dataj)


def read_cmd(client_socket):
      data = retrieve_data()
      print data
      client_socket.send(data)


def handle_cmd(client_socket):
    # buffer size 5Byte : command
    cmd = client_socket.recv(cmd_length)
    print '\n' + cmd 
    # if WRITE command, receive more data
    if cmd == WRITE_CMD:
      write_cmd(client_socket)
    # if READ command, send data.txt
    elif cmd == READ_CMD:
      read_cmd(client_socket)
    else:
      print '  not a valid cmd, now exit'
      return False
    return True 


# main()
def hmem_agent(ip, port):
  # open a socket
  srvsock = open_socket(ip, port)
  # handle connect
  while True:
    clisock, (remhost, remport) = srvsock.accept()
    rtn = handle_cmd(clisock);
    clisock.close()
    if(rtn == False):
      break  


def main():
    ip = '172.16.1.21' 
    ip = 'localhost' 
    port = 9997
    hmem_agent(ip,port)

if __name__ == '__main__':
    main()
    


