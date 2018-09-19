from celery import Celery
import sys
import subprocess

app = Celery('tasks', backend="redis://localhost", broker="amqp://")

def shell(cmd):
  h = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE).stdout
  out = h.read(); h.close()
  return out

@app.task
def probe(cmd, opt):
  pps = opt['pps'] if opt.has_key('pps') else 100
  if cmd == 'trace':
    method = opt['method'] if opt.has_key('method') else 'icmp-paris'
    date = shell('date +%Y%m%d-%H:%M:%S').strip()
    cmd = "scamper -c 'trace -P %s' -p %d -o - -O warts -f input | tee '%s.warts' | sc_analysis_dump -C" % (method, pps, date, )
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
