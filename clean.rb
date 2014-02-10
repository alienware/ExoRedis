require 'socket'
require 'logger'
#require 'eventmachine'

module ExoRedisServer
	LOG_FILE = "exoredis-server.log"
	PORT = 15000

	class << self

		def startup
			begin
				exoredis_server = TCPServer.open(PORT)
			rescue Errno::EADDRINUSE
				logs "ERROR: ExoRedis server cannot be started. Port #{PORT} is already in use."
				exit
			end

			loop {
				Thread.start(exoredis_server.accept) do |client|
					logs "#{client} connected at #{Time.now.ctime}."
					client.puts("Welcome to ExoRedis service.\nCurrent supported commands are GET, SET ...")
					process client
				end
			}
		end

		def process exoredis_client
			$h = File.open(ARGV[0], "r") { |io|
				Marshal.load(io)
			}
			while true
				input = exoredis_client.gets
				time = (Time.now.to_f*1000).to_i
				logs "#{exoredis_client} requested #{input}"
				tokens = input.split(" ")
				case tokens[0]
					when "GET"
						if $h[tokens[1]].nil? || time > $h[tokens[1]][1]
							$h.delete(tokens[1])
							exoredis_client.puts "nil"
						else
							exoredis_client.puts $h[tokens[1]][0]
						end

					when "SET"
						update = false
						case tokens[5]
							when "NX"
								update = true if $h[tokens[1]].nil?
								puts 'here'
							when "XX"
								update = true unless $h[tokens[1]].nil?
							when nil
								update = true
						end
						puts 'hi'
						if update
							$h[tokens[1]] = [tokens[2]]
							$h[tokens[1]] << time + tokens[3].to_i*1000 + tokens[4].to_i if tokens[3]
							puts $h
							exoredis_client.puts "OK"
						else
							exoredis_client.puts "NOT OK"
						end
						puts 'hello'
				end
			end
		end

		def logs msg
			(@logger ||= Logger.new(LOG_FILE)) << msg + "\n"
		end

		def cleanup
			@logger.close
		end
	end

end

begin
	ExoRedisServer.startup
rescue SystemExit, Interrupt
	File.open(ARGV[0], "w") { |io|
		Marshal.dump($h, io)
	}
	#TODO: Graceful client connection termination
	#exoredis_client.puts "Closing the connection. Bye!"
	#exoredis_client.close
end