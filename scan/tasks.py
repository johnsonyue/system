import subprocess
import threading
import sys
import json

from celery import Celery
from celery import Task
import pika

o = json.load(open('secrets.json'))
broker = o['broker']; backend = o['backend']
username = broker['username']; broker_ip = broker['IP_addr']
password = broker['password']; port = broker['port']
backend_ip = backend['IP_addr']

app = Celery(
  'tasks',
  backend = "redis://%s" % (backend_ip),
  broker = "amqp://%s:%s@%s:%s" % (username, password, broker_ip, port)
)

@app.task
def probe(cmd, opt):
  if not opt.has_key('input'):
    probe.update_state(state="FAILURE", meta={'reason': 'No input'})
    return
  inp = opt['input']
  pps = opt['pps'] if opt.has_key('pps') else 100
  if cmd == 'trace':
    method = opt['method'] if opt.has_key('method') else 'udp-paris'
    cstr = "scamper -c 'trace -P %s' -p %d -o - -O warts -f input | tee '%s' | sc_analysis_dump -C" % (method, pps, inp, )
    sys.stderr.write( cstr + "\n" )
    p = subprocess.Popen(cstr, shell=True, stdout=subprocess.PIPE)
    h = p.stdout

    cnt = 0
    while True:
      if not h.readline():
        h.close()
        break
      cnt += 1
      probe.update_state(state="PROGRESS", meta={'probed': cnt})
    p.wait()

def on_rcvd(ch, method, properties, body, p):
  print(" [x] Received %r" % body)
  if not body:
    p.stdin.close()
    p.wait()
    ch.stop_consuming()
    return
  p.stdin.write( body + '\n' )
  ch.basic_ack(delivery_tag = method.delivery_tag)

def reply(ch, p):
  h = p.stdout
  while True:
    l = h.readline().strip()
    if not l:
      ch.basic_publish(exchange='', routing_key = 'read', body = '')
      h.close()
      break
    ch.basic_publish(exchange='', routing_key = 'read', body = "%s" % (l))

@app.task
def listen():
  cstr = "sc_attach -p 54188 -i - -o - | sc_warts2text"
  sys.stderr.write( cstr + "\n" )
  p = subprocess.Popen(cstr, shell=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE)

  connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
  channel = connection.channel()


  channel.queue_declare(queue='write', auto_delete = True)
  channel.basic_consume(lambda ch, method, properties, body: on_rcvd(ch, method, properties, body, p), queue='write')

  t = threading.Thread(target=reply, args=(channel, p))
  t.start()

  channel.start_consuming()
