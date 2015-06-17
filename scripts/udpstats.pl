#!/usr/bin/perl

# Get the udp stats from netstat and calculate the error rate for the last second

my ($rec, $err, $rec_n, $err_n) = (0,0,0,0);

while (true) {
 
	my @stats = `netstat -s|grep Udp: -A4`;
#Udp:
#    851851898 packets received
#    4389457 packets to unknown port received.
#    3449421204 packet receive errors
#    5634268 packets sent 

	for my $l (@stats) {
		$rec_n = $1 if ($l =~ /(\d+) packets received/);
		$err_n = $1 if ($l =~ /(\d+) packet receive errors/);
	}
	my $r = 1.0* ($err-$err_n) / ($rec - $rec_n);
	print "Receive error rate $r\n";

	$rec = $rec_n;
	$err = $err_n;

	sleep(1);
}
