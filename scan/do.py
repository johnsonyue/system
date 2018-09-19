import subprocess
import os

from celery import Celery

# utils
def shell(cmd):
  f = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE).stdout
  out = f.read(); f.close()
  return out

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
def on_message(m):
  global h # must declare as global when modified in an callback
  status = m['status']
  if status == 'FAILURE':
    print "FAILURE: %s" % (m['result']['reason'] if m['result'].has_key('reason') else 'unknown')
    return
  if not h or h.poll():
    print 'sync'
    h = subprocess.Popen(sync, shell=True)

# start remote task 'trace'
celery = Celery()
celery.conf.update(
  broker_url = 'amqp://mngr:**@**:5672',
  result_backend = 'redis://**',
)
r = celery.send_task('tasks.probe', ['trace',{'input': inp}], queue='vp.hk01')
r.get( on_message=on_message, propagate=False )
