from tasks import probe

def on_message(m):
  print m

r = probe.apply_async(['trace',{}])
r.get( on_message=on_message, propagate=False )
