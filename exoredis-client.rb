require 'socket'

hostname = 'localhost'
port = 15000

s = TCPSocket.open(hostname, port)

while line = s.gets
	puts line.chop
end
s.close