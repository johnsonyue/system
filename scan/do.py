import subprocess
import threading
import os
import json
import sys

from celery import Celery
import pika

if len(sys.argv) < 2:
  exit()
task = sys.argv[1]

# utils
def shell(cmd):
  f = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE).stdout
  out = f.read(); f.close()
  return out

# callbacks
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
inp = "%s.warts" % (date)

# progress update callback
## global variables for callback.
h = None
sync = "./run.sh ssh %s sync -l %s -r %s" % ( "HKVPS", os.path.join( "this-time", inp ) , os.path.join( "/home/john", inp ) )
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
    print 'sync'
    h = subprocess.Popen(sync, shell=True)

# main
# start remote task
o = json.load(open('secrets.json'))
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
  r = celery.send_task('tasks.probe', ['trace',{'input': inp}], queue='vp.hk01')
  r.get( on_message=on_probe_message, propagate=False )

elif task == 'lg':
  # send task to add new client
  r = celery.send_task('tasks.listen', [], queue='vp.hk02')

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
