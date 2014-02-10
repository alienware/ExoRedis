require 'socket'
#require 'awesome_print'

begin

	$h = File.open(ARGV[0], "r") { |io|
		Marshal.load(io)
	}

	exoredis_server = TCPServer.open(15000)
	loop {
		Thread.start(exoredis_server.accept) do |exoredis_client|
			puts "#{exoredis_client} connected at #{Time.now.ctime}."
			exoredis_client.puts("Welcome to ExoRedis service.")
			exoredis_client.puts("Current supported commands are GET, SET ...")
			while input = exoredis_client.gets
				time = (Time.now.to_f*1000).to_i
				puts "#{exoredis_client} requested #{input}"
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
							when "XX"
								update = true unless $h[tokens[1]].nil?
							when nil
								update = true
						end
						if update
							$h[tokens[1]] = [tokens[2]]
							$h[tokens[1]] << time + tokens[3].to_i*1000 + tokens[4].to_i if tokens[3]
							exoredis_client.puts "OK"
						else
							exoredis_client.puts "NOT OK"
						end

				end
			end
			#TODO: Handle multi-threading
			#Thread.exit
		end
	}
rescue SystemExit, Interrupt
	File.open(ARGV[0], "w") { |io|
		Marshal.dump($h, io)
	}
	#TODO: Graceful client connection termination
	#exoredis_client.puts "Closing the connection. Bye!"
	#exoredis_client.close
end
