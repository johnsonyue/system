#!/bin/bash

target(){
  # methods:
  #   US: Uniform Sampling
  #   URS: Uniform Random Sampling
  cfg=$1
  python <(
  cat << "EOF"
import sys
import os
import json
import socket
import struct

# utils
def ip2int(ip):
  packedIP = socket.inet_aton(ip)
  return struct.unpack("!L", packedIP)[0]

def int2ip(i):
  return socket.inet_ntoa(struct.pack('!L',i))

# methods.
def us(prefix, density, offset):
  f = prefix.split('/')
  m = min( 32, max(8,int(f[1])) ) # mask
  g = 2**(32-max(density,m)) # granuality
  n = 2**(32-m) # network size
  a = ip2int(f[0])/n*n # start address
  for i in range( 0, n, g ):
    print int2ip( a + i + offset )

def urs(prefix, density):
  return

# main.
try:
  if os.path.exists(sys.argv[1]):
    cfg = json.load(open(sys.argv[1]))['target']
  elif not cfg:
    cfg = json.loads(sys.argv[1])
  else:
    cfg = {}
except Exception,e:
  sys.stderr.write("something wrong with config: %s\n" % (e) )
  cfg = {}

while True:
  try:
    l = raw_input().strip()
  except:
    break

  if not l:
    continue
  method = cfg["method"] if cfg.has_key("method") else "US"
  density = cfg["density"] if cfg.has_key("density") else 24
  if method == "US":
    offset = cfg["offset"] if cfg.has_key("offset") else 1
    us(l, density, offset)
  elif method == "URS":
    urs(l, density)
  # TODOS: add more methods.
EOF
  ) "$cfg"
}

creds(){
  python <(
  cat << "EOF"
import json
import sys

name = sys.argv[1]
l = json.load(open('secrets.json'))['nodes']
c = filter( lambda x: x['name'] == name, l )
if c:
  print "%s@%s|%d|%s|%s" % (c[0]['username'],c[0]['IP_addr'],c[0]['port'],c[0]['password'],c[0]['directory'])
EOF
  ) "$1"
}

filter(){
  python <(
  cat << "EOF"
import json
import sys

o = json.load(sys.stdin)

del o['nodes']
print json.dumps(o, indent=2)
EOF
  )
}
export filter

probe(){
  node_name=$1
  IFS="|" read _ _ _ dir< <(creds $node_name)
  ./run.sh ssh -n $node_name put -l $INPUT -r "$dir/$INPUT"
  #python do.py probe
}

usage(){
  echo "./run.sh <\$command> <\$args> [\$options]"
  echo "COMMANDS:"
  echo "  target -c <\$config_file> / <\$json_string>"
  echo ""
  echo "  task"
  echo ""
  echo "  ssh -n <\$node_name> <\$operation>"
  echo "    OPERATIONS:"
  echo "      setup"
  echo "      activate"
  echo "      cat -r <\$remote>"
  echo "      mkdirs -r <\$remote_dir>"
  echo "      get/put -l <\$local> -r <\$remote>"
  echo "      sync -l <\$local> -r <\$remote>"
  echo ""
  echo "  probe -n <\$node_name> -i <\$input> -o <\$output>"
  echo "    I/O TYPES:"
  echo "      # local & remote result file share the same name <\$result_file>"
  echo "      -i <\$target_file> -o <\$result_file>"
  echo "      -i - -o -"
  echo "      -i - -o <\$result_file>"
  exit
}

# parse options.
test $# -lt 1 && usage
args=""
while test $# -gt 0; do
  case "$1" in
    -n)
      NODE=$2
      shift 2
      ;;
    -i)
      INPUT=$2
      shift 2
      ;;
    -o)
      OUTPUT=$2
      shift 2
      ;;
    -l)
      LOCAL=$2
      shift 2
      ;;
    -r)
      REMOTE=$2
      shift 2
      ;;
    -c)
      CONFIG=$2
      shift 2
      ;;
    *)
      args="$args $1"
      shift
      ;;
  esac
done
eval set -- "$args"

# parse positional arguments.
cmd=$1
case $cmd in
  "target")
    target "$CONFIG" | sort -R
    ;;

  "task")
    ;;

  "ssh")
    test $# -lt 2 && usage
    operation=$2

    test -z "$NODE" && usage
    node_name="$NODE"

    # credentials.
    IFS="|" read ssh port pass dir< <(creds $node_name)

    # upload files.
    test "$operation" == "setup" && \
      ./run.sh ssh -n $node_name mkdirs -r $dir && \
      ./run.sh ssh -n $node_name put -l tasks.py -r $dir/tasks.py && \
        cat secrets.json | filter | \
      ./run.sh ssh -n $node_name cat -r $dir/secrets.json && echo
    # inline scripts.
    ( test "$operation" == "setup" || \
      test "$operation" == "activate" ) && \
    case $operation in
      "setup")
        cat << "EOF"
apt-get install -y python-pip rabbitmq-server redis-server
pip install -U celery "celery[redis]"
EOF
        ;;
      "activate")
        cat << "EOF"
celery worker -A my_app -l info
EOF
        ;;
    esac \
    | \
    # automatic ssh
    expect -c " \
      set timeout -1
      spawn bash -c \"ssh $ssh -p $port 'bash -s'\"
      expect -re \".*password.*\" {send \"$pass\r\"}
      while {[gets stdin line] != -1} {
        send \"\$line\r\"
      }
      send \004
      expect eof \
    "

    # automatic scp, rsync
    case $operation in 
      "put" | "get")
        test ! -z "$LOCAL" && test ! -z "$REMOTE" || usage
        from=$(test "$operation" == "put" && echo "$LOCAL" || echo "$ssh:$REMOTE")
        to=$(test "$operation" == "put" && echo "$ssh:$REMOTE" || echo "$LOCAL")
        expect -c " \
          set timeout -1
          spawn scp -P $port $from $to
          expect -re \".*password.*\" {send \"$pass\r\"}
          expect eof \
        "
        ;;
      "mkdirs")
        test -z "$REMOTE" && usage
        expect -c " \
          set timeout -1
          spawn bash -c \"ssh $ssh -p $port 'mkdir -p $REMOTE'\"
          expect -re \".*password.*\" {send \"$pass\r\"}
          expect eof \
        "
        ;;
      "cat")
        test -z "$REMOTE" && usage
        expect -c " \
          set timeout -1
          spawn bash -c \"ssh $ssh -p $port 'cat >$REMOTE'\"
          expect -re \".*password.*\" {send \"$pass\r\"}
          log_user 0
          while {[gets stdin line] != -1} {
            send \"\$line\r\"
          }
          send \004
          expect eof \
        "
        ;;
      "sync")
        test ! -z "$LOCAL" && test ! -z "$REMOTE" || usage
        expect -c " \
          set timeout -1
          spawn rsync -avrt --copy-links -e \"ssh -p $port\" $ssh:$REMOTE $LOCAL
          expect -re \".*password.*\" {send \"$pass\r\"}
          expect eof \
        "
        ;;
    esac
    ;;

  "probe")
    test ! -z "$INPUT" && test ! -z "$OUTPUT" || usage
    test -z "$NODE" && usage
    node_name="$NODE"

    test "$INPUT" == "-" && \
      lg $node_name $c || \
      probe $node_name
    ;;
  "*")
    usage
    exit
    ;;
esac
