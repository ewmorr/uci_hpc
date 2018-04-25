#!/bin/bash
#Eric Morrison
#032918
#parses output from trim scripts that implement trimmomatic and bbduk.sh on the UCI HPC
#USAGE: bash parse_trim_output.sh trimming_results.txt > output.txt

FILE=$1

SAMPID=$(grep 'SAMPLE' $FILE | cut -d ' ' -f 2)
READS=$(grep 'Result:' $FILE | cut -f 2 | cut -d ' ' -f 1)
BP=$(grep 'Result:' $FILE | cut -f 3 | cut -d ' ' -f 1)

#convert to array to loop
SAMPID=($SAMPID)
READS=($READS)
BP=($BP)

#echo "SampleID bp_remaining"
for i in "${!SAMPID[@]}"
do
echo ${SAMPID[$i]} ${READS[$i]} ${BP[$i]}
done

