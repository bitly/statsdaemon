#!/usr/bin/perl

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
	my $p_n = ($rec_n - $rec);
	my $r = 1.0* ($err_n - $err) / $p_n;
	print "Error rate $r\t packets $p_n\n" unless ($rec == 0 && $err == 0);

	$rec = $rec_n;
	$err = $err_n;

	sleep(3);
}
