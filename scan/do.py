import subprocess
import threading
import os
import json
import sys
import argparse

from celery import Celery
import pika

# positional arguments.
parser = argparse.ArgumentParser()
parser.add_argument('-n')
parser.add_argument('-f')
parser.add_argument('task', action='store')

p = parser.parse_args()
task = p.task
node_name = p.n
if not task or not node_name:
  exit()

if task == 'probe' and not p.f:
  exit()
target_file = p.f

# configurations.
o = json.load(open('secrets.json'))
nd = { n['name']: n for n in o['nodes'] }
if not nd.has_key(node_name):
  sys.stderr.write("no such node: %s\n" % (node_name))
nc = nd[node_name];
remote_dir = nc['directory']

lc = json.load(open('config.json'))
local_dir = lc['directory']

# callbacks
def shell(cmd):
  f = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE).stdout
  out = f.read(); f.close()
  return out

def on_reply(ch, method, properties, body):
  print("%r" % body)
  ch.basic_ack(delivery_tag = method.delivery_tag)
  if not body:
    ch.stop_consuming()

def listen():
  channel.queue_declare(queue='read', auto_delete = True)
  channel.basic_consume(on_reply, queue='read')
  channel.start_consuming()

# task parameters
date = shell('date +%Y%m%d-%H:%M:%S').strip()
output = "%s.warts" % (date)

# progress update callback
## global variables for callback.
h = None
sync = "./run.sh ssh -n %s sync -l %s -r %s" % ( "HKVPS", os.path.join( local_dir, output ) , os.path.join( remote_dir, output ) )
if h and not h.poll():
  h.wait()
## callback.
def on_probe_message(m):
  global h # must be declared as global if modified in a callback
  status = m['status']
  if status == 'FAILURE':
    print "FAILURE: %s" % (m['result']['reason'] if m['result'].has_key('reason') else 'unknown')
    return
  if not h or h.poll():
    h = subprocess.Popen(sync, shell=True)

# main
# start remote task
broker = o['broker']; backend = o['backend']
username = broker['username']; broker_ip = broker['IP_addr']
password = broker['password']; port = broker['port']
backend_ip = backend['IP_addr']

celery = Celery()
celery.conf.update(
  broker_url = "amqp://%s:%s@%s:%s" % (username, password, broker_ip, port),
  result_backend = "redis://%s" % (backend_ip)
)

if task == 'probe':
  # upload target file
  target_file_basename = os.path.basename( target_file )
  remote_target_file = os.path.join( remote_dir, target_file_basename )
  put = "./run.sh ssh -n %s put -l %s -r %s" % ( "HKVPS", target_file, remote_target_file )
  h = subprocess.Popen(put, shell=True)
  h.wait()

  # send task to begin probe
  r = celery.send_task( 'tasks.probe', ['trace',{'input': remote_target_file, 'output': output}], queue="vp.%s.probe" % (node_name) )
  r.get( on_message=on_probe_message, propagate=False )
  if h and not h.poll():
    h.wait()
  h = subprocess.Popen(sync, shell=True)
  h.wait()

elif task == 'lg':
  # send task to add new client
  r = celery.send_task( 'tasks.listen', [], queue="vp.%s.lg" % (node_name) )

  credentials = pika.PlainCredentials(username, password)
  connection = pika.BlockingConnection(pika.ConnectionParameters(broker_ip, credentials=credentials))
  channel = connection.channel()

  t = threading.Thread(target=listen)
  t.start()

  # write
  channel.queue_declare(queue='write', auto_delete = True)
  while True:
    try:
      l = raw_input()
    except:
      #print(" [x] Sent ''")
      channel.basic_publish(exchange='', routing_key='write', body='')
      break
    channel.basic_publish(exchange='', routing_key='write', body=l)
    #print(" [x] Sent %s" % (l) )

  t.join()
  connection.close()
