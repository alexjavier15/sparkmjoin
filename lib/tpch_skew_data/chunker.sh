#!/bin/bash
echo "$# parameters";  #- this prints number of parameters
echo "$@"; # - this prints actual parameters
CHUNK_SIZE=`expr $1 \* 1024 \* 1024`
SO=".tbl"
DEST_DIR="/home_local/rivas/sparkmjoin/data/tpch50"
SUFFIX=".csv"
NUM_CHUNKS=1
shift


echo "Files $@"; # - this prints actual parameters

ceiling_divide() {
NUM_CHUNKS=$((($1+$2-1)/$2))
}

for file in $@;
do
file_size=`stat --printf="%s" $file$SO`
ceiling_divide $file_size $CHUNK_SIZE
echo "Number fo chunks for $file : $NUM_CHUNKS"
split -d -a 2  -n l/$NUM_CHUNKS -e  $file$SO $file"_"
for chunk in $file"_"*

do
	echo $chunk
	mv $chunk $DEST_DIR/$chunk$SUFFIX
done
done

