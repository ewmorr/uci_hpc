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

sub find_seq_chunk{
    my $fastqRef = $_[0];
    my @fastq = @$fastqRef;
    my $header = $_[1];
    
    my $offset;
    for(my $i = 1; $i<@fastq; $i++){
        if($fastq[$i] =~ /^$header/){
            $offset = $i;
            last;
        }
    }
    return($offset);
}

sub get_ids{
    my($headerLine, $header) = @_;
    
    $headerLine =~ /($header.+)\s.+/;
    my $pairID = $1;
    $headerLine =~ /$header.+\s(1|2):.+/;
    my $read = $1;
    return($pairID, $read);
}

sub process_fastq{
    my $in = $_[0];
    my $header;
    my @fastq;
    my %fastqPairs;
    my @sequenceOrder;
    
    open(IN, "$in") || die "Can't open input fastq\n";
    LINE: while (my $line = <IN>){
        chomp $line;
    
        if($. == 1){
            $line =~ /(@.+?):/;
            $header = $1;
        }
    
        if($line =~ /^.+($header).+$/){
            my @line = split($header, $line);
            for(my $i = 1; $i<@line; $i++){
                $line[$i] = $header.$line[$i];
            }
            push(@fastq, @line);
        }else{
            push(@fastq, $line);
        }
        
        if(@fastq > 4){
            my $offset = find_seq_chunk(\@fastq, $header);

            if($offset < 4){
            print "$fastq[0] does not have four elements. Skipping sequence...\n";
            splice(@fastq, 0, $offset);
            next LINE;
            }else{
                my @seq = splice(@fastq, 0, $offset);
                if(length($seq[1]) != length($seq[3])){
                    print "$seq[0] has unequal length quality and sequence lines. Skipping sequence...\n";
                    next LINE;
                }
                
                my($pairID, $read) = get_ids($seq[0], $header);
                $fastqPairs{$pairID}{$read} = [@seq];
                
                if(defined($sequenceOrder[0]) == 0){
                    push(@sequenceOrder, $pairID);
                }else{
                    if($sequenceOrder[-1] ne $pairID){
                        push(@sequenceOrder, $pairID);
                    }
                }
            }
        }
    }
    if(scalar(@fastq) == 4){
        my @seq = splice(@fastq, 0, 4);
        if(length($seq[1]) != length($seq[3])){
            print "$seq[0] has unequal length quality and sequence lines. Skipping sequence...\n";
        }else{
            my($pairID, $read) = get_ids($seq[0], $header);
            $fastqPairs{$pairID}{$read} = [@seq];
            if($sequenceOrder[-1] ne $pairID){
                push(@sequenceOrder, $pairID);
            }
        }
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
    
    my($fastqPairsRef, $sequenceOrderRef) = process_fastq($in);
    check_for_paired_reads_and_print($fastqPairsRef, $sequenceOrderRef, $pairs, $singles);
}
