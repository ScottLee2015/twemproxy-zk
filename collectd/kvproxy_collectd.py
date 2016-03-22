#! /usr/bin/env python


import collectd
import json
import socket
import sys
from random import randint

PLUGIN_NAME = 'distkvproxy'
VERBOSE_LOGGING = True

def log_verbose(msg):
  if VERBOSE_LOGGING:
    collectd.info('{} [verbose]: {}'.format(PLUGIN_NAME, msg))


class KVProxyPlugin(object):
  """
    This class collects KV proxy stats info, and passes to Collectd.
  """

  def __init__(self, ip=None, port=None):
    self.ips = []
    self.ports = []

    if ip and port:
      self.ips.append(ip)
      self.ports.append(port)

    # default interval is 20 seconds.
    self.interval = 10 
    self.plugin_name = PLUGIN_NAME
    self.test = False
    self.per_server_stats = False


  def config(self, conf):
    """
      Config callback.

      Collectd will call this method to init the plugin.

      :param conf: a Config object containing all info representing the
                   config tree.
      example config section:

      <Module redisproxy_collectd>
        proxy    "ip1:port1" "ip1:port2" "ip1:port3"
        interval 20
        verbose  true/false
      </Module>

      For how to interpret config object, see here:
      https://collectd.org/documentation/manpages/collectd-python.5.shtml
    """
    collectd.info('!!now config kvproxy {}'.format(conf))
    for node in conf.children:
      key = node.key.lower()

      collectd.info('key: {0: <12}, value: {1: <12}'.format(key, node.values))

      if key == 'proxy':
        for s in node.values:
          tp = s.split(':')
          if len(tp) == 2:
            self.ips.append(tp[0])
            self.ports.append(int(tp[1]))
          else:
            collectd.warning('KVProxyPlugin: invalid proxy address %s' % s)
      elif key == 'interval':
        self.interval = float(node.values[0])
      elif key == 'verbose':
        global VERBOSE_LOGGING
        if node.values[0]:
        #in ['true', 'True']:
          VERBOSE_LOGGING = True
        else:
          VERBOSE_LOGGING = False
      elif key == 'test':
        # if we are in test mode
        self.test = node.values[0]
      elif key == 'perserverstats':
        # should we report per-dbserver stats?
        self.per_server_stats = node.values[0]
      else:
        collectd.warning('KVProxyPlugin: Unkown configuration key %s'
                         % node.key)
    log_verbose('have inited plugin {}'.format(self.plugin_name))


  def submit(self, type, type_instance, value, instance):
    """
      dispatch a msg to collectd.
    """
    #plugin_instance = '{}:{}'.format(server, port)
    plugin_instance = instance

    v = collectd.Values()
    v.plugin = self.plugin_name
    v.plugin_instance = plugin_instance
    v.type = type
    v.type_instance = type_instance
    v.values = []
    if isinstance(value, list):
      if self.test:
        for i in range(len(value)):
          value[i] = randint(50, 100)
      log_verbose('value is list: {}'.format(value))
      v.values.extend(value)
    else:
      if self.test:
        value = randint(50, 100)
      v.values = [value, ]

    log_verbose('submit value: {}'.format(v.values))

    try:
      v.dispatch()
    except Exception as e:
      collectd.info('failed to dispatch data {}:{}: {}'.format(
                    type_instance, value, e))


  def parse_server(self, sname, server, instance):
    """
      Parse proxy stats info about a server, then send to collectd

      :param sname:  backend server name
      :param server: json obj representing a server stats.
      :param ip:      proxy ip address
      :param port:    proxy port
    """
    self.submit('server_connections',
                sname,
                str(server['server_connections']),
                instance)
    self.submit('server_eof',
                sname,
                str(server['server_eof']),
                instance)
    self.submit('server_err',
                sname,
                str(server['server_err']),
                instance)
    self.submit('req',
                sname,
                str(server['requests']),
                instance)
    self.submit('reqbytes',
                sname,
                str(server['request_bytes']),
                instance)
    self.submit('resp',
                sname,
                str(server['responses']),
                instance)
    self.submit('respbytes',
                sname,
                str(server['response_bytes']),
                instance)
    self.submit('in_queue',
                sname,
                str(server['in_queue']),
                instance)
    self.submit('in_queue_bytes',
                sname,
                str(server['in_queue_bytes']),
                instance)
    self.submit('out_queue',
                sname,
                str(server['out_queue']),
                instance)
    self.submit('out_queue_bytes',
                sname,
                str(server['out_queue_bytes']),
                instance)


  def parse_pool(self, pname, pool, instance):
    """
      Parse proxy stats info about a KV pool, then send to collectd

      :param pname: pool name
      :param pool:  json obj representing pool stats.
      :param ip:    proxy ip address
      :param port:  proxy port
    """
    # Record top summaries for this pool.
    self.submit('client_connections', pname,
                pool['client_connections'],
                instance)
    self.submit('client_err', pname,
                pool['client_err'],
                instance)
    self.submit('client_eof', pname,
                pool['client_eof'],
                instance)
    self.submit('server_ejects', pname,
                pool['server_ejects'],
                instance)
    self.submit('forward_error', pname,
                pool['forward_error'],
                instance)
    self.submit('fragments', pname,
                pool['fragments'],
                instance)
    self.submit('req', pname,
                pool['total_requests'],
                instance)
    self.submit('reqbytes', pname,
                pool['total_requests_bytes'],
                instance)
    self.submit('resp', pname,
                pool['total_responses'],
                instance)
    self.submit('respbytes', pname,
                pool['total_responses_bytes'],
                instance)
    self.submit('latency_min', pname,
                pool['latency_min'],
                instance)
    self.submit('latency_max', pname,
                pool['latency_max'],
                instance)
    self.submit('latency_p50', pname,
                pool['latency_p50'],
                instance)
    self.submit('latency_p90', pname,
                pool['latency_p90'],
                instance)
    self.submit('latency_p95', pname,
                pool['latency_p95'],
                instance)
    self.submit('latency_p99', pname,
                pool['latency_p99'],
                instance)


  def send_stats_to_collectd(self, content):

    proxy_stats = json.loads(content)
    
    for pk in proxy_stats.keys():
      stats = proxy_stats[pk]
      
      for k in sorted(stats.keys()):
        # Only look into kv-pools, skip high-level summary stats.
        v = stats[k]
        if type(v) is dict:
          # Now 'k' is pool name, 'v' is object representing the pool.
          self.parse_pool(k, v, pk)

          # Check if we should report per-dbserver stats.
          if not self.per_server_stats:
            continue

          # Look into each server in the pool.
          for bk in v.keys():
            # TODO: summarize counts / errors of all servers.
            if type(v[bk]) is dict:
              # Now 'bk' is backend server name
              self.parse_server(bk, v[bk], pk)


  def read_proxy_stats(self):
    """
      Get actual data from proxy, pass them to Collectd.

    """
    log_verbose('start one round of kvproxy collection')
    for i in range(len(self.ips)):
      ip = self.ips[i]
      port = self.ports[i]
      try:
        log_verbose('will read stats at {}:{}'.format(ip, port))
        conn = socket.create_connection((ip, port))
        conn.send('agent_r')
        buf = True
        content = ''
        while buf:
          buf = conn.recv(4096)
          if buf:
            content += buf
        conn.close()
        log_verbose(buf) 
        self.send_stats_to_collectd(content);
      except Exception as e:
        log_verbose('Error:: {}'.format(e))


def main():
  ip = '192.168.0.158'
  port = 31000
  proxy = KVProxyPlugin(ip, port)
  proxy.read_proxy_stats()

if __name__ == '__main__':
  main()
  sys.exit(0)

proxy = KVProxyPlugin()
collectd.register_config(proxy.config)
collectd.register_read(proxy.read_proxy_stats, proxy.interval)
