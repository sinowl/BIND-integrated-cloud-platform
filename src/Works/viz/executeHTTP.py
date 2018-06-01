from BaseHTTPServer import HTTPServer, BaseHTTPRequestHandler
import sys
import time

#PORT=80
#ADDR=''
class MyHandler(BaseHTTPRequestHandler):
        msg = None
        def do_GET(self):
                self.send_response(200)
                self.send_header('Content-type', 'text/html')
                self.end_headers()
                self.send_body()

        def send_body(self):
                #msg =  '<html><body>access time : <b>{}</b></body></html>'.format( time.asctime() )
                self.wfile.write( self.msg.encode() )
        @staticmethod
        def startMsg():
                MyHandler.msg = '<html><body>'

        @staticmethod
        def insertMsg(m):
                MyHandler.msg += '{}'.format(m)
	
        @staticmethod
        def endMsg():
                MyHandler.msg += '</body></html>'

#MyHandler.startMsg()
#MyHandler.insertMsg( time.asctime() )
#MyHandler.insertMsg( "hi" )
#MyHandler.endMsg()

class Viz:
	def __init__(self):
		self.PORT=8000
		self.ADDR=''
		MyHandler.startMsg()
	
	def insertMSG(self, msg ):
		MyHandler.insertMsg( msg )

	def initHTTP(self):
		MyHandler.endMsg()
		self.httpd = HTTPServer( (self.ADDR, self.PORT), MyHandler)
		print 'listening on port' + self.ADDR+':'+ str(self.PORT)

	def startHTTP(self):
		self.httpd.handle_request()

	def startHTTPforever(self):
		self.httpd.serve_forever()

	def endHTTP(self):
		self.httpd.server_close()

if __name__=='__main__':
	viz = Viz()
	viz.insertMSG( time.asctime() )
	viz.insertMSG( "hi" )
	viz.initHTTP()
	#while True:
	viz.startHTTP()	
	#	viz.endHTTP()
