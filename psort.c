#include <sys/stat.h>
#include <sys/mman.h> 
#include <string.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/sysinfo.h>
#include <pthread.h>
#include <unistd.h>

#define MAX 500000000
#define ERRMSG  "An error has occurred\n"

typedef struct {
	int l;
	int r;
} mergeArgs;

int key[MAX];
int keyid[MAX];

void merge(int l, int m, int r)
{

    int *arr = key;
    int *arrid = keyid;    
    int i, j, k;
    int n1 = m - l + 1;
    int n2 = r - m;
 
    //adapt basic mergesort temp arrays with key ids
    //for writing output
    int L[n1], R[n2];
    int Lid[n1], Rid[n2];
 
    for (i = 0; i < n1; i++){
        L[i] = arr[l + i];
        Lid[i] = arrid[l + i];
	//printf("Lid: %d\n", Lid[i]);
    }
    for (j = 0; j < n2; j++) {
        R[j] = arr[m + 1 + j];
        Rid[j] = arrid[m + 1 + j];
	//printf("Rid: %d\n", Lid[i]);
    }	
    /* Merge the temp arrays back into arr[l..r]*/
    i = 0; // Initial index of first subarray
    j = 0; // Initial index of second subarray
    k = l; // Initial index of merged subarray
    while (i < n1 && j < n2) {
        if (L[i] <= R[j]) {
            arr[k] = L[i];
	    arrid[k] = Lid[i];
            i++;
        }
        else {
            arr[k] = R[j];
	    arrid[k] = Rid[j];
            j++;
        }
        k++;
    }
 
    /* Copy the remaining elements of L[], if there
    are any */
    while (i < n1) {
        arr[k] = L[i];
	arrid[k] = Lid[i];
        i++;
        k++;
    }
 
    /* Copy the remaining elements of R[], if there
    are any */
    while (j < n2) {
        arr[k] = R[j];
	arrid[k] = Rid[j];
        j++;
        k++;
    }
}
//basic mergesort structure
void mergeSort(int l, int r)
{
    if (l < r) {
        // Same as (l+r)/2, but avoids overflow for
        // large l and h
        int m = l + (r - l) / 2;
 
        // Sort first and second halves
        mergeSort(l, m);
        mergeSort(m + 1, r);
 
        merge(l, m, r);
    }
}
 
//initializes the child process to run their local mergesort
void *mergeThread(void *args) {
        mergeArgs *child = args;
	int l = child->l;
	int r = child->r;

	mergeSort(l, r);
        return (void *) 0;//return null
}

//writes out characters to a file, one by one based on the associated keys from
//the sorted array
//uses the fact that mmap data is untouchedvoid writeOut(int numRecs, char* mapped, char* output, int arr[], int arrid[]) {

void writeOut(int numRecs, char* mapped, char* output) {

    FILE *out;
    out = fopen(output, "w");
    for (int i = 0; i < numRecs; i++) {
	    for (int j = 0; j < 100; j++) {
		char c = mapped[(keyid[i]*100)+j];
		fprintf(out, "%c", c);
	    }
	//printf("id: %d/%d\n", arrid[i], arr[i]);

    }

    fsync(fileno(out));

    fclose(out);


	return;
}
//checks for primitive condition if we need more than one process
void runChild(int numRecs) {
	//create children to sort parts of tree, then sort the last part
	//hardcoded for consistency, maybe look into variable sort if time
	if (numRecs >= 4) {

    		pthread_t child[4];

		int L[4];
		int R[4];

        	int m = 0 + ((numRecs - 0) / 2);
		int lm = 0 + ((m - 0) / 2);
		int rm = m + ((numRecs - m) / 2);

		L[0] = 0;
		L[1] = lm + 1;
		L[2] = m + 1;
		L[3] = rm + 1;

		R[0] = lm;
		R[1] = m;
		R[2] = rm;
		R[3] = numRecs;

		mergeArgs *chargv[4];

		for (int k = 0; k < 4; k++) {
			chargv[k] = malloc(sizeof(mergeArgs));	
		}

		for (int j = 0; j < 4; j++) {
			
		   chargv[j]->l = L[j];
		   chargv[j]->r = R[j];


    		   if (pthread_create(&child[j], NULL, mergeThread, chargv[j])) {
			//perror("error creating child");
			exit(1);
    		   }
	       }
		//wait for children to finish
    		for (int i = 0; i < 4; i++) {
			pthread_join(child[i], NULL);
			free(chargv[i]);
    		}

		//3 more sorts to do
		merge(0, lm, m);
	        merge(m+1, rm, numRecs);
		merge(0, m, numRecs);


	} else {
		mergeSort(0, numRecs);
	}



	return;
}

int main (int argc, char *argv[]){

   int fd;

   struct stat s;
    size_t size = 100;
    size_t fsize;
    char* mapped;
    int i;
    //note sure how to handle extreme amounts of records so hard set value for now
    //int key[MAX];
    //int keyid[MAX];

    if (argc > 3) {
	   return -1;
    }
    //checking if valid file open
    if ((fd = open (argv[1], O_RDONLY)) < 0) {
	write(STDERR_FILENO, ERRMSG, strlen(ERRMSG));
                                exit(0);	    
    }
    //grabbing status of file
    if (( fstat (fd, & s)) < 0) {
	    return 0;
    }
    fsize = s.st_size;

   // printf("size of input file is %ld\n", fsize);

    // actual size of records we are dealing with
    int numRecs = fsize/size;

    if (numRecs < 1) {
	write(STDERR_FILENO, ERRMSG, strlen(ERRMSG));
	exit(0);
    }
    //printf("%d\n", numRecs);
      if ((mapped = (char*) mmap (0, fsize, PROT_READ, MAP_PRIVATE, fd, 0)) < 0) {
	 return -1;
      }
    //test code
    /*printf("before:\n");
    for (i = 0; i < numRecs; i++) {
	char *string;
	string = strndup(&mapped[i*100], 100);
	printf("%s\n", string);
    }*/
    //grabbing keys from the records
    int *keyptr = (int*)mapped;
    

    //setup for sorting, keyid is used for
    //grabbing original mapped bytes for
    //output file, key is for sorting
    for (i = 0; i < numRecs; i++) {
	    int tmpkey = keyptr[i*25];
	    key[i] = tmpkey;
	    keyid[i] = i;
	   // printf("id: %d/%d\n",keyid[i],key[i]);
    }
    //creation of child thread to handle sort 
    runChild(numRecs - 1);
    /*
    pthread_t child[1];
    pthread_t ch1;
    child[0] = ch1;
    mergeArgs *child1 = malloc(sizeof (mergeArgs));
    child1->arr = key;
    child1->arrid = keyid;
    child1->l = 0;
    child1->r = numRecs-1;
     
    if (pthread_create(&child[0], NULL, mergeThread, child1)) {
	//perror("error creating child");
	exit(1);
    }

    for (int i = 0; i < 1; i++) {
	pthread_join(child[0], NULL);
    }
*/

    /*printf("after:\n");
for (i = 0; i < numRecs; i++) {
    char *string;
    string = strndup(&mapped[i*100], 100);
    printf("%s\n", string);
}*/

    //write to file
    writeOut(numRecs, mapped, argv[2]);
    close(fd);
    return 0;
}
