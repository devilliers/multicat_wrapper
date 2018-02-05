#!/usr/bin/ruby

#Turn warnings off
$VERBOSE = nil

source_file = nil
destination = nil
d_val = nil
e_val = nil
b_val = nil
i_val = nil
source_file = ARGV[0]
destination = ARGV[1]
d_val = ARGV[2]
e_val = ARGV[3]
b_val = ARGV[4]
i_val = ARGV[5]

cmd = "/home/elemental/multicat -x -U #{source_file} #{destination}" 

if (d_val != 'nil' && d_val != nil)
  cmd << " -D #{d_val}"
end
 
if (e_val != 'nil' && e_val != nil)
  cmd << " -E #{e_val}"
end

if (b_val != 'nil' && b_val != nil)
  cmd << " -B #{b_val}"
end

if (i_val != 'nil' && i_val != nil)
  cmd << " -I #{i_val}"
end

if (source_file == nil || destination == nil) 
  puts "Usage: loop_multicat.rb <source>.ts <destination_ip>:<destination_port> D E B I"
else
  puts "cmd:#{cmd}"
  puts `#{cmd}` while (true)
end

