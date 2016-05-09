#!/bin/bash
echo "$# parameters";  #- this prints number of parameters
echo "$@"; # - this prints actual parameters


#cloud20
DATA_FOLDER="/home_local/rivas/sparkmjoin/data/tpch_skew"
SCHEMA_FOLDER="/home_local/rivas/sparkmjoin/data/tpch"
FILE_FORMAT="csv"
REL_FORMAT="pf"
NUM_CHUNKS=0

create_pfFile(){
echo "$REL_FILE"

echo -n $'{\t"file_name": "my_file_name",\n\t"file_size": 200000,\n\t"num_records": 100,\n\t"chunk_locations": [\n' > "$REL_FILE"

}

append_chunk_to_pfFile(){
CHUNK_ID=$1
CHUNK_FILE=$2

CHUNK_ID_FORAMATED="$(printf "%04d" $1 )"

echo -n $'\t{ "id": "'>> "$REL_FILE"
echo -n "$CHUNK_ID_FORAMATED">> $REL_FILE
echo -n '", "holder_location": "' >> $REL_FILE
echo -n "$CHUNK_FILE" >> $REL_FILE
echo -n $'" }' >>$REL_FILE

if [ $CHUNK_ID -ne $NUM_CHUNKS ];
then
echo -n $',' >> $REL_FILE
fi

echo -n $'\n'>> $REL_FILE

}
append_schema_pfFile(){

echo -n $'\t],\n\t"schema_location": "'$DDL_FILE$'"\n}'>> $REL_FILE
}

generate_json_chunk(){
CHUNK_ID_FORAMATED="$(printf "%04d" $1 )"
CHUNK_ID_SUFFIX="$(printf "%02d" $1 )"
CHUNK_FILE=$2
touch $CHUNK_FILE
#TODO

echo -n $'{\t"parent_file": ''"'$REL_FILE'",' > $CHUNK_FILE
echo -n $'\n\t"chunk_id": '>> $CHUNK_FILE
echo -n '"'$CHUNK_ID_FORAMATED'",'>> $CHUNK_FILE
echo -n $'\n\t"chunk_size": 100000,\n\t"num_records": 50,\n\t"data_location": '  >> $CHUNK_FILE
echo '"'"$FILE_PATH"'_'"$CHUNK_ID_SUFFIX.$FILE_FORMAT"'"' >> $CHUNK_FILE
echo '}' >> $CHUNK_FILE

}
#call generator.exe to create table script and data file




for file in $DATA_FOLDER/*.json
do
echo "Removing $file"
rm $file
done

for file in $DATA_FOLDER/*.$REL_FORMAT
do
echo "Removing $file"
rm $file
done
 
for relation in $@
do
echo "*********Creating pf files for $relation************"
TABLE_NAME=$relation
FILE_PATH="$DATA_FOLDER/$TABLE_NAME"
DDL_FILE="$SCHEMA_FOLDER/$TABLE_NAME.ddl"
REL_FILE="$FILE_PATH.$REL_FORMAT"

NUM_CHUNKS=0
for chunk in $DATA_FOLDER/$relation"_"*
do
echo $chunk
NUM_CHUNKS=`expr $NUM_CHUNKS + 1`
done
TOT_CHUNKS=$NUM_CHUNKS
NUM_CHUNKS=`expr $NUM_CHUNKS - 1`


CHUNK_ID=0


echo "NUM of chunks for $relation is $TOT_CHUNKS"
create_pfFile
until [ $CHUNK_ID -gt $NUM_CHUNKS ]
do 

CHUNK_FILE=$FILE_PATH'_'$CHUNK_ID'.json'



#Generate a file json file representing the chunk
generate_json_chunk $CHUNK_ID $CHUNK_FILE
#append the chunk information to the pf master file

append_chunk_to_pfFile $CHUNK_ID $CHUNK_FILE
CHUNK_ID=`expr $CHUNK_ID + 1`
done

#append the schema info to the pf master file
append_schema_pfFile

done


