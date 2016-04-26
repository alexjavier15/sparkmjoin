#!/bin/bash
echo "$# parameters";  #- this prints number of parameters
echo "$@"; # - this prints actual parameters


#cloud20
DATA_FOLDER="/home/alex/sparkmjoin/data"
FILE_FORMAT="csv"
REL_FORMAT="pf"
NUM_CHUNKS=$1
echo "Number of Chunks : $NUM_CHUNKS"
shift
TABLE_NAME="$1"
TUPLES_PER_CHUNK=`expr $4 / $NUM_CHUNKS`
echo "Number of tuples per chunk : $TUPLES_PER_CHUNK"

PREFIX_ATTR=${TABLE_NAME:0:1}

FILE_PATH="$DATA_FOLDER/$TABLE_NAME"
DDL_FILE="$FILE_PATH.ddl"
REL_FILE="$FILE_PATH.$REL_FORMAT"
#TODO recompile the generator
ORIGINAL_PATH="/home/alex/sparkmjoin/data/data.txt"

#cloud 20
EXEC_PATH="/home/alex/sparkmjoin/lib/generator"

#name of the executable
GENERATOR="generator.exe"
set -- "${@:1:3}" "$TUPLES_PER_CHUNK" "${@:5}"
ATTRS="$@";
echo "New ATTR"
echo "$@"; # - this prints actual parameters

if [ ! -d "$DATA_FOLDER" ]; then
    # Control will enter here if $DIRECTORY doesn't exist.
    echo "data folder doesn't exist"
    mkdir "$DATA_FOLDER" 
else
  echo "folder exists" 
fi


generate_ddl()
{
echo "$DDL_FILE"
echo "$PREFIX_ATTR"
echo -n $'{\t"type": "struct",\n\t"fields": [\n\t\t{"name": "id", "type": "integer", "nullable": false},\n\t\t{"name": "' > "$DDL_FILE"
echo -n "$PREFIX_ATTR"'1"' >> $DDL_FILE 
echo -n $', "type": "integer", "nullable": false}]\n}' >> "$DDL_FILE"


}
create_pfFile(){
echo "$REL_FILE"

echo -n $'{\t"file_name": "my_file_name",\n\t"file_size": 200000,\n\t"num_records": 100,\n\t"chunk_locations": [\n' > "$REL_FILE"

}

append_chunk_to_pfFile(){
CHUNK_ID=$1
CHUNK_FILE=$2

echo -n $'\t{ "id": "000'>> "$REL_FILE"
echo -n "$CHUNK_ID">> $REL_FILE
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
CHUNK_ID=$1
CHUNK_FILE=$2
touch $CHUNK_FILE
#TODO

echo -n $'{\t"parent_file": ''"'$REL_FILE'",' > $CHUNK_FILE
echo -n $'\n\t"chunk_id": '>> $CHUNK_FILE
echo -n '"000'$CHUNK_ID'",'>> $CHUNK_FILE
echo -n $'\n\t"chunk_size": 100000,\n\t"num_records": 50,\n\t"data_location": '  >> $CHUNK_FILE
echo '"'"$FILE_PATH"'_'"$CHUNK_ID.$FILE_FORMAT"'"' >> $CHUNK_FILE
echo '}' >> $CHUNK_FILE

}
#call generator.exe to create table script and data file


generate_ddl 
create_pfFile
CHUNK_ID=1
if [ -f "$ORIGINAL_PATH" ];
then
	echo "File $FILE exist."
	rm $ORIGINAL_PATH
fi


until [ $CHUNK_ID -gt $NUM_CHUNKS ]

do

CHUNK_FILE=$FILE_PATH'_'$CHUNK_ID'.json'

#call generator data
$EXEC_PATH/$GENERATOR $ATTRS
#copy to the data set workspace

cp $ORIGINAL_PATH $FILE_PATH'_'"$CHUNK_ID.$FILE_FORMAT"
#remove the generated data
rm $ORIGINAL_PATH
#Generate a file json file representing the chunk
generate_json_chunk $CHUNK_ID $CHUNK_FILE
#append the chunk information to the pf master file
append_chunk_to_pfFile $CHUNK_ID $CHUNK_FILE
CHUNK_ID=`expr $CHUNK_ID + 1`
done

#append the schema info to the pf master file
append_schema_pfFile




