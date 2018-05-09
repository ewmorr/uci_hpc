#!/usr/bin/perl
#Eric Morrison
#050918
#Script to clean erroneous reads appends from LSA

use strict;
use warnings;

#SUBS
sub usage{
    print STDERR q(
Usage: perl clean_LSA_paritions.pl [input fastq] [output fastq for paired reads interleaved] [output fastq for singletons]
    
The LSA algorithm appears to erroneously append header lines from some reads to the previous qual line. This script finds erroneous appends and removes any read pairs where one read has sequence and qual lines that are not of equal length, or the read entry does not have four lines.

Paired reads and singleton reads are printed to separate files.
    
);
    exit;
}

sub open_fastq_find_head{
    my $in = $_[0];
    my $header;
    my @fastq;
    open(IN, "$in") || die "Can't open input fastq\n";
    while (my $line = <IN>){
        chomp $line;
        if($. == 1){
            $line =~ /(@.+?):/;
            $header = $1;
        }
        #print $line, "\n";
        push(@fastq, $line);
    }
    my $fastq = join("splitLinesHere", @fastq);
    return(\$fastq, $header);
}

sub check_for_eq_seq_qual_len{
    my $fastqRef = $_[0];
    my @fastq = @$fastqRef;
    my $header = $_[1];
    
    my %fastqPairs;
    my @sequenceOrder;
    foreach my $seq (@fastq){
        my @seq = split("splitLinesHere", $seq);
        $seq[0] = $header.$seq[0];
       
        if(scalar(@seq) != 4){
            print "$seq[0] does not have four elements. Skipping sequence...\n";
            next;
        }
        if(length($seq[1]) != length($seq[3])){
            print "$seq[0] has unequal length quality and sequence lines. Skipping sequence...\n";
            next;
        }
        
        $seq[0] =~ /($header.+)\s.+/;
        my $pairID = $1;
            
        if(defined($sequenceOrder[0]) == 0){
            push(@sequenceOrder, $pairID);
        }else{
            if($sequenceOrder[-1] ne $pairID){
                push(@sequenceOrder, $pairID);
            }
        }
        $seq[0] =~ /$header.+\s(1|2):.+/;
        my $read = $1;
        $fastqPairs{$pairID}{$read} = [@seq];
    }
    return(\%fastqPairs, \@sequenceOrder);
}

sub check_for_paired_reads_and_print{
    my $fastqPairsRef = shift @_;
    my $sequenceOrderRef = shift @_;
    my($pairs, $singles) = @_;
    open(PAIRS, ">$pairs") || die "Can't open output for paired sequences.\n";
    open(SING, ">$singles") || die "Can't open output for singleton sequences.\n";
    my %fastqPairs = %$fastqPairsRef;
    my @sequenceOrder = @$sequenceOrderRef;
    
    foreach my $head (@sequenceOrder){
        if(defined($fastqPairs{$head}{"1"}) == 1 and defined($fastqPairs{$head}{"2"}) == 1){
            foreach my $fastqLines (@{ $fastqPairs{$head}{"1"} }){
                print PAIRS $fastqLines, "\n";
            }
            foreach my $fastqLines (@{ $fastqPairs{$head}{"2"} }){
                print PAIRS $fastqLines, "\n";
            }
        }else{
            foreach my $read (keys %{ $fastqPairs{$head} }){
                foreach my $fastqLines (@{ $fastqPairs{$head}{$read} }){
                    print SING $fastqLines, "\n";
                }
            }
        }
    }
    
}

#MAIN
{
    if(@ARGV == 0 || $ARGV[0] eq "-h"){
        &usage;
    }
    my $in = $ARGV[0];
    my $pairs = $ARGV[1];
    my $singles = $ARGV[2];
    my($fastqRef, $header) = open_fastq_find_head($in);
    my @fastq = split($header, $$fastqRef);
    shift @fastq;
    
    my($fastqPairsRef, $sequenceOrderRef) = check_for_eq_seq_qual_len(\@fastq, $header);
    check_for_paired_reads_and_print($fastqPairsRef, $sequenceOrderRef, $pairs, $singles);
}
