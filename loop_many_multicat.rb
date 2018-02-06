#!/usr/bin/ruby

source = nil
destination = nil

source = ARGV[0]
destination = ARGV[1]

ip,port = destination.split(":") unless destination.nil?

if (source == nil || destination == nil || !File.directory?(source) || ip == nil || ip.length < 1 || port == nil || port.length < 1)
  puts "Usage: loop_many_multicat.rb <directory of source files> <destination_ip>:<destination_port>"
else
  files = Dir.glob("#{source}/*.{ts,m2ts,TRP}")
  if !files || files.length < 1
    puts "No files found in #{source}/"
  else
    puts files
    port = port.to_i
    if (1024 > port)
      puts "Port must be > 1024"
    else
      # special case for foxtel stream filenames
      sorted = files.sort do |a,b|
        a =~ /(\d+)-.*/
        a = $1
        b =~ /(\d+)-.*/
        b = $1
        a.to_i <=> b.to_i
      end
      while(true)
        threads = []
        sorted.each_with_index do |file,i|
          threads << Thread.new do
	    `multicat -u -U -X #{file} #{ip}:#{port+i}`
 	    #`loop_multicat.rb #{file} #{ip}:#{port+i}`
          end
        end
        threads.each {|t| t.join }
      end
    end
  end
end
