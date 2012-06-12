import sys
import os
import json
import cgi
from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer
from datetime import datetime
import time
import math
import subprocess

import socket
IP_ADDRESS = socket.gethostbyname(socket.gethostname())
QUERY_PREFIX = '/query'
STATUS_PREFIX = '/status'

def rrd_values(datestring, rrdpath):
    proc = subprocess.Popen(
        ['rrdtool',
         'fetch',
         rrdpath,
         'AVERAGE',
         '-r', '3600',
         '-s', 'midnight %s' % datestring,
         '-e', 's+1day',
         ],
        stdout=subprocess.PIPE,
        )
    output = proc.stdout.read()

    output_data = []
    for line in output.splitlines():
        line = line.strip()
        try:
            thedate_secs, value = line.split(': ', 1)
        except ValueError:
            continue
        thedate = datetime.fromtimestamp(float(thedate_secs))
        # for some reason, rrdtool gives us .5 hours too
        if thedate.minute != 0:
            continue
        value = float(value)
        if math.isnan(value):
            continue
        output_data.append(
            [time.mktime(thedate.timetuple()), value],
            )
    return output_data

class Server(BaseHTTPRequestHandler):

    def do_GET(self):
        if self.path.startswith(QUERY_PREFIX):
            print "Got QUERY request"
            qs = self.path[len(QUERY_PREFIX)+1:]
            qs = cgi.parse_qs(qs)
            if 'date' not in qs:
                self.send_response(500)
                print "Got request with no date" 
                return
            datestring = qs.get('date')[0]
            if 'rrdfile' not in qs:
                self.send_response(500)
                print "Got request with no rrdfile" 
                return
            rrdfile = qs.get('rrdfile')[0]

            data = rrd_values(datestring, 
                              os.path.join(self.server.rrdpath, rrdfile))
            self.send_response(200)
            self.send_header("Content-type", "text/plain")
            self.end_headers()
            self.wfile.write(json.dumps(data, indent=2))
        else:
            # OK status to show we are running
            self.send_response(200)
            self.send_header("Content-type", "text/plain")
            self.end_headers()
            self.wfile.write("OK")
            
def usage():
    print 'Usage: %s port /path/to/rrd/files' % sys.argv[0]
    sys.exit(1)
      
if __name__ == "__main__":
    if len(sys.argv) != 3:
        usage()

    port = int(sys.argv[1])
    rrdpath = sys.argv[2]
    if not os.path.exists(rrdpath):
        print "Path does not exist: %s" % rrdpath
        usage()

    print "Listening on %s:%s" % (IP_ADDRESS, port)
    server = HTTPServer((IP_ADDRESS, port), Server)
    server.rrdpath = rrdpath
    server.serve_forever()

