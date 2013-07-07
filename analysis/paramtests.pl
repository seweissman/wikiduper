unless($#ARGV >= 0){
    die "Usage: paramtests.pl <test output directory>\n";
}

$dir = $ARGV[0];
print "(* nhash, k, n l, bits, score *)\n";
for $resultdir (<$dir/*>){
    if($resultdir =~ /-(\d+)-(\d+)-(\d+)-(\d+)-(\d+)-scores$/){
	$nhash = $1;
	$k = $2;
	$n = $3;
	$l = $4;
	$bits = $5;
	$goodct = 0;
	$badct = 0;
	for $file (<$resultdir/*>){
	    if($file =~ /_SUCCESS/){
		next;
	    }
	    #print "$file\n";
	    #print "nhash $nhash\n";
	    #print "k $k\n";
	    #print "n $n\n";
	    #print "l $l\n";
	    #print "bits $bits\n";
	    open(FILEIN,"<$file");
	    for $line (<FILEIN>){
		$line =~ /(\d+)\t(\d+)/;
		$result = $1;
		$count = $2;
		#print "result = $result, count = $count\n";
		if($result == 1){
		    $goodct = $count;
		}else{
		    $badct = $count;
		}
	    }

	}
	$total = $goodct + $badct;
	$fprate = $badct*1.0/$goodct;
	if($bits == 60){
	    print "$nhash, $k, $n, $l, $bits, $fprate\n";
	    print "$nhash, $k, $n, $l, $bits, $goodct\n";
	    print "$nhash, $k, $n, $l, $bits, $badct\n";
	}
    }
}
