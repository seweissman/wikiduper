
if($#ARGV < 0){
    die "Usage: allpairsjaccard.pl <europarl>\n";
}

$europarl = $ARGV[0];

open(FILEIN,"<$europarl");

$ct = 0;
@lines = <FILEIN>;
my $line1;
my $line2;
my $sim;
for($i=0;$i<=$#lines;$i++){
    $line1 = $lines[$i];
    chomp $line1;
    for($j=0;$j<=$#lines;$j++){
	$line2 = $lines[$j];
	chomp $line2;
	$sim = jaccard($line1,$line2);
	printf "%d,%d,%.3f\n",$i+1,$j+1,$sim;
    }
}

sub jaccard{
    my $line1 = lc($_[0]);
    my $line2 = lc($_[1]);
    my @line1words = split("\\W",$line1);
    my @line2words = split("\\W",$line2);
    my %line1set;
    my %line2set;
    
    for $w (@line1words){
	if($w =~ /\S/){
	    #print "WORD: $w\n";
	    $line1set{$w} = 1;
	}
    }

    for $w (@line2words){
	if($w =~ /\S/){
	    #print "WORD: $w\n";
	    $line2set{$w} = 1;
	}
    }
    my $sharect = 0;
    my $totalct;
    $set1 = keys %line1set;
    $set2 = keys %line2set;
    for $w (keys %line1set){
	if($line2set{$w}){
	    $sharect++;
	}
    }
    $totalct = $set1 + $set2 - $sharect;
    #print "LINE1: $line1\n";
    #print "LINE2: $line2\n";
    #print "SHARECT: $sharect\n";
    #print "TOTALCT: $totalct\n";
    my $sim = $sharect*1.0/$totalct;
    if($sim > 1.0){
	die "Something wrong. Similarity can't be gt 1.0.\n$line1\n$line2\n";
    }
    return $sim;


}
