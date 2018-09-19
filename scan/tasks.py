from celery import Celery
import sys
import subprocess

app = Celery('tasks', backend="redis://**", broker="amqp://mngr:**@**")

@app.task
def probe(cmd, opt):
  if not opt.has_key('input'):
    probe.update_state(state="FAILURE", meta={'reason': 'No input'})
    return
  inp = opt['input']
  pps = opt['pps'] if opt.has_key('pps') else 100
  if cmd == 'trace':
    method = opt['method'] if opt.has_key('method') else 'icmp-paris'
    cmd = "scamper -c 'trace -P %s' -p %d -o - -O warts -f input | tee '%s' | sc_analysis_dump -C" % (method, pps, inp, )
    sys.stderr.write( cmd + "\n" )
    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
    h = p.stdout

    cnt = 0
    while True:
      if not h.readline():
        h.close()
        break
      cnt += 1
      probe.update_state(state="PROGRESS", meta={'probed': cnt})
    p.wait()
