#define _GNU_SOURCE
#define _FILE_OFFSET_BITS 64
#define _LARGEFILE_SOURCE

#include<stdio.h>
#include<stdlib.h>
#include<string.h>
#include<math.h>
#include <unistd.h>

#define MAX 99999
#define MIN 0
char cwd[1024];
//my machine
//char data_path[]="/home/renata/data";

//cloud 20
char data_path[]="/home_local/rivas/sparkmjoin/data";

char getRandomChar();
char* getRandomString(int size);
int computeStringSize(int numbers, int numLength, int string, int tupleSize);
int* distribute(int columns, int howMany);

void generateINT(const char *relname, int tuples, int columns, int min_value, int max_value, int isWithId);
void generateFLOAT(const char *relname, int tuples, int columns, int min_value, int max_value, int isWithId);
void generateSTRING(const char *relname, int tuples, int columns, int width, int isWithId);
void generateSTRING2(const char *relname, int tuples, int columns, int tuple_width, int isWithId);
void generateMIX(const char *relname, int tuples, int columns, int how_many, int tuple_size, int min_value, int max_value, int isWithId);
void generateMIXNumbers(const char *relname, int tuples, int columns, int how_many, int min_value, int max_value, int isWithId);


void createTableINT(const char *relation, int columns, int isWithId);
void createTableFLOAT(const char *relation, int columns, int isWithId);
void createTableSTRING(const char *relation, int string_len, int columns, int isWithId);
void createTableMIX(const char *relation, int *positions, int string_len, int columns, int howMany, int isWithId);
void createTableMIXNum(const char *relation, int *positions, int columns, int howMany, int isWithId);

int main(int argc, char** argv)
{
	char *relname;

	int method;	
	int tuples;
	int columns;
	int width;
	int how_many;
	int min_value;
	int max_value;
	int with_id; /* is first column tupleID (int) 1- true, 0 - false */

	if(argc == 1)
	{
		fprintf(stderr,"Usage: <executable> <NAME> <0|1>(withID)	0 (INT) 		<tuples> <columns> <min> <max> [default: min = 0 max = 99999]\n");
		fprintf(stderr,"       <executable> <NAME> <0|1>(withID)	1 (FLOAT) 		<tuples> <columns> <min> <max> [default: min = 0 max = 99999]\n");
		fprintf(stderr,"       <executable> <NAME> <0|1>(withID)	2 (STRING) 		<tuples> <columns> <width> \n");
		fprintf(stderr,"       <executable> <NAME> <0|1>(withID)	3 (STRING2) 	<tuples> <columns> <tuple_width> \n");
		fprintf(stderr,"       <executable> <NAME> <0|1>(withID)	4 (MIX) 		<tuples> <columns> <strings> <tuple_size> <min> <max> [default: min = 0 max = 99999]\n");
		fprintf(stderr,"       <executable> <NAME> <0|1>(withID)	5 (MIX) 		<tuples> <columns> <floats>  <min> <max> [default: min = 0 max = 99999]\n");
		exit(0);
	}


    if (getcwd(cwd, sizeof(cwd))!=NULL)
	   fprintf(stdout, "Current working dir: %s\n", cwd);

	relname = argv[1];
	with_id = atoi(argv[2]);
	method = atoi(argv[3]);
	tuples = atoi(argv[4]);
	columns = atoi(argv[5]);

	if( (method == 0 || method == 1) && argc == 8 )
	{
		min_value = atoi(argv[6]);
		max_value = atoi(argv[7]);
	}
	else if( (method == 0 || method == 1))
	{
		min_value = MIN;
		max_value = MAX;
	}

	if( (method == 2 || method == 3 )  && argc == 7 )
	{
		width = atoi(argv[6]);
	}

	if( (method == 4 )  && argc == 10 )
	{

		how_many = atoi(argv[6]);
		width = atoi(argv[7]);
		min_value = atoi(argv[8]);
		max_value = atoi(argv[9]);
	}
	else if( (method == 4 )  && argc == 8 )
	{
		how_many = atoi(argv[6]);
		width = atoi(argv[7]);
		min_value = MAX;
		max_value = MIN;
	}
	else if( (method == 5 )  && argc == 7 )
	{

		how_many = atoi(argv[6]);
		min_value = MIN;
		max_value = MAX;
	}
	else if( (method == 5 )  && argc == 9 )
	{

		how_many = atoi(argv[6]);
		min_value = atoi(argv[7]);
		max_value = atoi(argv[8]);
	}

	switch(method)
	{
		case 0:
			/* last parameter means whether we want first column to represent ID, 1=true*/
			generateINT(relname, tuples, columns, min_value, max_value, with_id);
			break;
		case 1:
			generateFLOAT(relname, tuples, columns, min_value, max_value, with_id);
			break;
		case 2:
			generateSTRING(relname, tuples, columns, width, with_id);
			break;
		case 3:
			generateSTRING2(relname, tuples, columns, width, with_id);
			break;
		case 4:
			generateMIX(relname, tuples, columns, how_many, width, min_value, max_value, with_id);
			break;
		case 5:
			generateMIXNumbers(relname, tuples, columns, how_many, min_value, max_value, with_id);
			break;

		default:

			break;
			

	}

	return 0;

}


void generateINT(const char *relname, int tuples, int columns, int min_value, int max_value, int isWithId)
{
	FILE *fp;
	char filename[256];
	char buffer[32];
	double estimated_size = 0;
	int temp = 0;
	int i, j;

	//sprintf(filename,"INT_out%d_%d.txt",tuples,columns);
	
	//sprintf(filename,"%s/data/data.txt", cwd);
	sprintf(filename,"%s/data.txt", data_path);

	printf("\nGenerate file with INT attributes:");
	printf("Number of Columns: %d\n",columns);
	printf("Number of Tuples: %d\n",tuples);
	printf("Integer values: [%d, %d]\n",min_value, max_value);


	if((fp = fopen(filename, "w+")) == NULL)
	{
		printf("Error opening file: %s\n",filename);
		return;
	} 

	srand((unsigned)time(0));
	for(i = 0; i < tuples; i++)
	{
		for(j = 0; j < columns; j++)
		{
			if (j==0 && isWithId)
				temp = i+1;  /* first tuple will be tuple ID */
			else
				temp = rand()%max_value;

			sprintf(buffer,"%d",temp);
			estimated_size += (int)strlen(buffer);
			fprintf(fp,"%d",temp);
			if( j < columns - 1)
			{
				fprintf(fp,",");
				estimated_size += 1;
			}
		}
		fprintf(fp,"\n");
		estimated_size += 1;
	}

	printf("Estimated file size: %lf MB\n",estimated_size / (1024 * 1024));
	printf("Average tuple size: %lf\n",(estimated_size - tuples) / tuples);
	printf("Output filename: %s\n",filename);
	printf("\n");

	createTableINT(relname, columns, isWithId);

}




void generateFLOAT(const char *relname, int tuples, int columns, int min_value, int max_value, int isWithId)
{
	FILE *fp;
	char filename[256];
	char buffer[32];
	double estimated_size = 0;
	float temp = 0;
	int i, j;

	//sprintf(filename,"FLOAT_out%d_%d.txt",tuples,columns);
	/* data file*/
	//sprintf(filename,"%s/data/data.txt", cwd);
	sprintf(filename,"%s/data.txt", data_path);

	printf("\nGenerate file with FLOAT attributes:");
	printf("Number of Columns: %d\n",columns);
	printf("Number of Tuples: %d\n",tuples);
	printf("Float values: [%d, %d]\n",min_value, max_value);


	if((fp = fopen(filename, "w+")) == NULL)
	{
		printf("Error opening file: %s\n",filename);
		return;
	} 
	srand((unsigned)time(0));

	for(i = 0; i < tuples; i++)
	{
		for(j = 0; j < columns; j++)
		{	if (j==0 && isWithId){
				int temp = i+1;  /* first column will be tuple ID (int) */
				sprintf(buffer,"%d",temp);
				estimated_size += (int)strlen(buffer);
				fprintf(fp,"%d",temp);
			}else{ /* regular float */
				temp = (rand()/((float)(32767) + 1)) *(max_value - min_value) + min_value;
				sprintf(buffer,"%.3f",temp);
				estimated_size += (int)strlen(buffer);
				fprintf(fp,"%.3f",temp);
			}

			if( j < columns - 1)
			{
				fprintf(fp,",");
				estimated_size += 1;
			}
		}
		fprintf(fp,"\n");
		estimated_size += 1;
	}

	printf("Estimated file size: %lf MB\n",estimated_size / (1024 * 1024));
	printf("Average tuple size: %lf\n",(estimated_size - tuples) / tuples);
	printf("Output filename: %s\n",filename);
	printf("\n");

	createTableFLOAT(relname, columns, isWithId);

}


void generateSTRING(const char *relname, int tuples, int columns, int attribute_width, int isWithId)
{
	char filename[256];
	char buffer[32];
	FILE *fp;
	int i,j;
	double estimated_size = 0;

	//sprintf(filename,"STRING_out%d_%d_%d.txt",tuples,columns,attribute_width);
	/* data file*/
	//sprintf(filename,"%s/data/data.txt", cwd);
        sprintf(filename,"%s/data.txt", data_path);

	printf("\nGenerate file with STRING attributes:");
	printf("Number of Columns: %d\n",columns);
	printf("Number of Tuples: %d\n",tuples);
	printf("Attribute_width: %d\n",attribute_width);


	if((fp = fopen(filename, "w+")) == NULL) {
		printf("Error opening file: %s\n",filename);
		return;
	} 
	srand((unsigned)time(0));

	for(i = 0; i < tuples; i++)
	{
		for(j = 0; j < columns; j++)
		{	if (j==0 && isWithId){
				int temp = i+1;  /* first column will be tuple ID */
				sprintf(buffer,"%d",temp);
				estimated_size += (int)strlen(buffer);
				fprintf(fp,"%d",temp);
			}else{
				char *temp = getRandomString(attribute_width);
				estimated_size += (int)strlen(temp);
				fprintf(fp,"%s",temp);
				free(temp);
				temp = NULL;
			}


			if( j < columns - 1)
			{
				fprintf(fp,",");
				estimated_size += 1;
			}

		}
		estimated_size += 1;
		fprintf(fp,"\n");
	}

	printf("Estimated file size: %lf MB\n",estimated_size / (1024 * 1024));
	printf("Average tuple size: %lf\n",(estimated_size - tuples) / tuples);
	printf("Output filename: %s\n",filename);
	printf("\n");

	createTableSTRING(relname, attribute_width, columns, isWithId);
}

void generateSTRING2(const char *relname, int tuples, int columns, int tuple_width, int isWithId)
{
	char filename[256];
	char buffer[32];
	FILE *fp;
	int i,j;
	double estimated_size = 0;
	int width;

	//sprintf(filename,"STRING2_out%d_%d_%d.txt",tuples,columns,tuple_width);

	/* data file*/
	//sprintf(filename,"%s/data/data.txt", cwd);
	sprintf(filename,"%s/data.txt", data_path);

	printf("\nGenerate file with STRING attributes:");
	printf("Number of Columns: %d\n",columns);
	printf("Number of Tuples: %d\n",tuples);
	printf("Attribute_width: %d\n",tuple_width);


	if((fp = fopen(filename, "w+")) == NULL) {
		printf("Error opening file: %s\n",filename);
		return;
	} 
	srand((unsigned)time(0));

	width = (double) tuple_width / (double) columns;
	for(i = 0; i < tuples; i++)
	{
		for(j = 0; j < columns; j++)
		{	if (j==0 && isWithId){
				int temp = i+1;  /* first column will be tuple ID */
				sprintf(buffer,"%d",temp);
				estimated_size += (int)strlen(buffer);
				fprintf(fp,"%d",temp);
			}else{
				char *temp = getRandomString(width);
				estimated_size += (int)strlen(temp);
				fprintf(fp,"%s",temp);
				free(temp);
				temp = NULL;
			}
			if( j < columns - 1)
			{
				fprintf(fp,",");
				estimated_size += 1;
			}

		}
		estimated_size += 1;
		fprintf(fp,"\n");
	}

	printf("Estimated file size: %lf MB\n",estimated_size / (1024 * 1024));
	printf("Average tuple size: %lf\n",(estimated_size - tuples) / tuples);
	printf("Output filename: %s\n",filename);
	printf("\n");

	createTableSTRING(relname, width, columns, isWithId);
}


void generateMIX(const char *relname, int tuples, int columns, int how_many, int tuple_size, int min_value, int max_value, int isWithId)
{
	char filename[256];
	FILE *fp;
	int i,j;
	int string_len;
	int count = 0;
	double estimated_size = 0;
	char *temp;
	int temp2;
	char buffer[32];
	int *positions;

	sprintf(buffer,"%d",max_value);
	string_len = computeStringSize(columns - how_many, (int)strlen(buffer), how_many, tuple_size);

	//sprintf(filename,"MIX_out%d_%d_%d.txt",tuples,columns,tuple_size);
	/* data file*/
	//sprintf(filename,"%s/data/data.txt", cwd);
	sprintf(filename,"%s/data.txt", data_path);

	printf("\nGenerate file with STRING attributes:");
	printf("Number of Columns: %d\n",columns);
	printf("Number of Tuples: %d\n",tuples);
	printf("Size of Tuples: %d\n",tuple_size);
	printf("String Length: %d\n",string_len);
	printf("Integer values: [%d, %d]\n",min_value, max_value);

	if((fp = fopen(filename, "w+")) == NULL)
	{
		printf("Error opening file: %s\n",filename);
		return;
	} 
	srand((unsigned)time(0));

	positions = distribute(columns, how_many);
	for(i = 0; i < tuples; i++)
	{
		for(j = 0; j < columns; j++)
		{
			if (j==0 && isWithId){
				int temp = i+1;  /* first column will be tuple ID */
				sprintf(buffer,"%d",temp);
				estimated_size += (int)strlen(buffer);
				fprintf(fp,"%d",temp);
			}else if ( positions[j] ) /* string column */
			{
				temp = getRandomString(string_len);
				estimated_size += (int)strlen(temp);
				fprintf(fp,"%s",temp);
				count++;
			}
			else /* otherwise int */
			{
				temp2 = rand() % max_value; 			
				sprintf(buffer,"%d",temp2);
				estimated_size += (int)strlen(buffer);
				fprintf(fp,"%d",temp2);
			}
			if( j < columns - 1)
			{		
				fprintf(fp,",");
				estimated_size += 1;
			}
		}
		fprintf(fp,"\n");
		estimated_size += 1;
		count = 0;
	}

	printf("Estimated file size: %lf MB\n",estimated_size / (1024 * 1024));
	printf("Average tuple size: %lf\n",(estimated_size - tuples) / tuples);
	printf("Output filename: %s\n",filename);
	printf("\n");

	createTableMIX(relname, positions, string_len, columns, how_many, isWithId);

}

void generateMIXNumbers(const char *relname, int tuples, int columns, int how_many, int min_value, int max_value, int isWithId)
{
	char filename[256];
	FILE *fp;
	int i,j;
	int count = 0;
	double estimated_size = 0;
	float temp;
	int temp2;
	char buffer[32];
	int *positions;

	sprintf(buffer,"%d",max_value);

	//sprintf(filename,"MIXNum_out%d_%d_%d.txt",tuples,columns, how_many);
	/* data file*/
	//sprintf(filename,"%s/data/data.txt", cwd);
	sprintf(filename,"%s/data.txt", data_path);

	printf("\nGenerate file with INT + FLOAT attributes:");
	printf("Number of Columns: %d\n",columns);
	printf("Number of Tuples: %d\n",tuples);
	printf("Size of FLOATS: %d\n",how_many);
	printf("Integer values: [%d, %d]\n",min_value, max_value);

	if((fp = fopen(filename, "w+")) == NULL)
	{
		printf("Error opening file: %s\n",filename);
		return;
	}

	srand((unsigned)time(0));
	positions = distribute(columns, how_many);
	for(i = 0; i < tuples; i++)
	{
		for(j = 0; j < columns; j++)
		{	if (j==0 && isWithId){
				int temp = i+1;  /* first column will be tuple ID */
				sprintf(buffer,"%d",temp);
				estimated_size += (int)strlen(buffer);
				fprintf(fp,"%d",temp);
			}else if ( positions[j] )
			{	/* generate float */
				temp = (rand()/((float)(32767) + 1)) *(max_value - min_value) + min_value;
				sprintf(buffer,"%.3f",temp);
				estimated_size += (int)strlen(buffer);
				fprintf(fp,"%.3f",temp);
				count++;
			}
			else
			{	/* generate int */
				temp2 = rand() % max_value;
				sprintf(buffer,"%d",temp2);
				estimated_size += (int)strlen(buffer);
				fprintf(fp,"%d",temp2);
			}
			if( j < columns - 1)
			{
				fprintf(fp,",");
				estimated_size += 1;
			}
		}
		fprintf(fp,"\n");
		estimated_size += 1;
		count = 0;
	}

	printf("Estimated file size: %lf MB\n",estimated_size / (1024 * 1024));
	printf("Average tuple size: %lf\n",(estimated_size - tuples) / tuples);
	printf("Output filename: %s\n",filename);
	printf("\n");

	createTableMIXNum(relname, positions,  columns, how_many, isWithId);

}


char getRandomChar()
{
	static const char input[] = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
	return input[rand() % (sizeof(input) - 1)];
}


char* getRandomString(int size)
{
	char *ret;
	int i;	
	ret = (char*)malloc((size + 1) * sizeof(char));
	for (i = 0; i < size; i++)
		ret[i] = getRandomChar();
	ret[size] = '\0';
	return ret;
}


int computeStringSize(int numbers, int numLength, int string, int tupleSize)
{
	int tmp1 = numbers * numLength;
	int tmp2 = tupleSize - tmp1;

	return (int)(tmp2 / string);
}

/* this method distributes 'howMany' columns to be of a specific type, out of all 'columns' columns*/
int* distribute(int columns, int howMany)
{
	int i;
	int *which;
	int step;
	int count = 0;
	
	which = (int *) malloc(columns * sizeof(int));
	/*	if (howMany == 0)
			step = columns + 1;
	else if (howMany > columns)
			step = 1;
	else{
		 every step(th) column will be of a special type
		step = (int) floor ((double)columns / (double)howMany);

	}


	if (step == 1)
		step = 2;

	for ( i = 0; i < columns; i++)
	{
		if( (i + (step / 2)) % step == 0 && count < howMany)
		{
			which[i] = 1;
			count++;
		}
		else
			which[i] = 0;
	}*/

	while ( count < howMany)
	{
		i = rand() % columns;
		if((which[i] != 1) && (i!= 0)) /* first column is reserved for tuple ID*/
		{
			which[i] = 1;
			count++;
		}
	}

	return which;
}


void createTableINT(const char *relation, int columns, int isWithId)
{
	FILE *fp;	
	char filename[256];
	int j;

	//sprintf(filename,"createINT%d.sql",columns);
	/* create table file*/
	//sprintf(filename,"%s/data/create.sql", cwd);
	sprintf(filename,"%s/create.sql", data_path);
	if((fp = fopen(filename, "w+")) == NULL) {
		printf("Error opening file: %s\n",filename);
		return;
	}

	fprintf(fp,"CREATE TABLE %s (",relation);
	for(j = 0; j < columns; j++)
	{	if(j==0 && isWithId)
			fprintf(fp,"c%d int primary key",(j + 1));
		else
			fprintf(fp,"c%d int",(j + 1));
		if( j < columns - 1)
			fprintf(fp,",");
	}
	fprintf(fp,");");
	fclose(fp);
}

void createTableFLOAT(const char *relation, int columns, int isWithId)
{
	FILE *fp;	
	char filename[256];
	int j;

	//sprintf(filename,"createFLOAT%d.sql",columns);
	/* create table file*/
	//sprintf(filename,"%s/data/create.sql", cwd);
	sprintf(filename,"%s/create.sql", data_path);

	if((fp = fopen(filename, "w+")) == NULL) {
		printf("Error opening file: %s\n",filename);
		return;
	}

	fprintf(fp,"CREATE TABLE %s (",relation);
	for(j = 0; j < columns; j++)
	{
		if(j==0 && isWithId)
			fprintf(fp,"c%d int primary key",(j + 1));
		else
			fprintf(fp,"c%d float4",(j + 1));

		if( j < columns - 1)
			fprintf(fp,",");
	}
	fprintf(fp,");");
	fclose(fp);
}

void createTableSTRING(const char *relation, int string_len, int columns, int isWithId)
{
	FILE *fp;	
	char filename[256];
	int j;

	//sprintf(filename,"createSTRING%d_%d.sql",columns, (columns * string_len));
	/* create table file*/
	//sprintf(filename,"%s/data/create.sql", cwd);
	sprintf(filename,"%s/create.sql", data_path);

	if((fp = fopen(filename, "w+")) == NULL) {
		printf("Error opening file: %s\n",filename);
		return;
	}

	fprintf(fp,"CREATE TABLE %s (",relation);
	for(j = 0; j < columns; j++)
	{	if(j==0 && isWithId)
			fprintf(fp,"c%d int primary key",(j + 1));
		else
			fprintf(fp,"c%d VARCHAR(%d)",(j + 1), string_len);
		if( j < columns - 1)
			fprintf(fp,",");
	}
	fprintf(fp,");");
	fclose(fp);
}

void createTableMIX(const char *relation, int *positions, int string_len, int columns, int howMany, int isWithId)
{
	FILE *fp;	
	char filename[256];
	int j;

	//sprintf(filename,"createMix%d_%d_%d.sql",columns, howMany, string_len);
	/* create table file*/
	//sprintf(filename,"%s/data/create.sql", cwd);
	sprintf(filename,"%s/create.sql", data_path);

	if((fp = fopen(filename, "w+")) == NULL) {
		printf("Error opening file: %s\n",filename);
		return;
	}

	fprintf(fp,"CREATE TABLE %s (",relation);
	for(j = 0; j < columns; j++)
	{	if(j==0 && isWithId)
			fprintf(fp,"c%d int primary key",(j + 1));
		else if ( positions[j] == 1)
			fprintf(fp,"c%d VARCHAR(%d)",(j + 1),string_len + 1);
		else
			fprintf(fp,"c%d int",(j + 1));
		if( j < columns - 1)
			fprintf(fp,",");
	}
	fprintf(fp,");");
	fclose(fp);
}

void createTableMIXNum(const char *relation, int *positions, int columns, int howMany, int isWithId)
{
	FILE *fp;	
	char filename[256];
	int j;

	//sprintf(filename,"createMixNum%d_%d.sql",columns, howMany);
	/* create table file*/
	//sprintf(filename,"%s/data/create.sql", cwd);
	sprintf(filename,"%s/create.sql", data_path);
	if((fp = fopen(filename, "w+")) == NULL) {
		printf("Error opening file: %s\n",filename);
		return;
	}
	
	fprintf(fp,"CREATE TABLE %s (",relation);
	for(j = 0; j < columns; j++)
	{	if(j==0 && isWithId)
			fprintf(fp,"c%d int primary key",(j + 1));
		else if ( positions[j] == 1)
			fprintf(fp,"c%d float",(j + 1));
		else
			fprintf(fp,"c%d int",(j + 1));

		if( j < columns - 1)
			fprintf(fp,",");
	}
	fprintf(fp,");");
	fclose(fp);
}
