/*
Andrew Hoch

This program takes as input a text file
Outputs to the console each word in the file and its number of occurences

The program makes use of multithreading

sample console instructions:

    $ gcc BagOfWords.c
    $ gcc a.out "test.txt"
*/


#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <unistd.h>
#include <assert.h>
#include <string.h>

#define			QUEUE_SIZE		100
#define 		TRUE			1
#define			FALSE			0
#define			CHUNK_SIZE		100
#define 		N_THREADS 		1	//just consumer threads

typedef struct word_bag {
	char 	*word;
	int		freq;
	struct word_bag *next;
} word_bag_t;
typedef struct {
	char buf[CHUNK_SIZE + 1];	// add one more byte to hold
								// the zero for zero-terminated string
	word_bag_t *head;			// head of the list
	int tid;					// thread id
	int hasrun;
} thread_info_t;
typedef struct {
	char buf[CHUNK_SIZE + 1];
	int used;
} item_t;

thread_info_t thread_info[N_THREADS];	// per thread information
			// pthread info
pthread_t threads[N_THREADS];
int sentence = 0;						// count the sentence (i.e., chunk) number
										// valid only for single threaded case




pthread_mutex_t the_mutex;
item_t queue[QUEUE_SIZE];
int num_elem = 0;
int next_index = 0;
pthread_cond_t condc, condp;
int producer_done = FALSE;

int pcount = 1;
int endingconsumer = 0;

word_bag_t *find_word(word_bag_t *head, char *word) {
	if (head == NULL) {
		return NULL;
	} else {
		for (word_bag_t *t = head; t != NULL; t = t->next) {
			if (strcmp(t->word, word) == 0) {
				return t;
			}
		}
		return NULL;
	}
}
word_bag_t *new_word(word_bag_t **head, char *word) {
	word_bag_t *bag = (word_bag_t *) malloc(sizeof(word_bag_t));
	bag->next = NULL;
	bag->word = word;
	bag->freq = 0;

	bag->next = *head;
	*head = bag;
	return bag;
}
void count_word(word_bag_t **head, char *buf, int start, int end) {
	assert(end >= start);
	char *new_word_str = (char *) malloc(end - start + 2);
	strncpy(new_word_str, &buf[start], end - start + 1);
	new_word_str[end - start + 1] = 0;

	// add it to linked list if it does not exist
	// or find where it is in the linked list;
	word_bag_t *word = find_word(*head, new_word_str);
	if (word == NULL) {
		printf("\tnew word [%s]\n", new_word_str);
		word = new_word(head, new_word_str);
	} else {
		free(new_word_str);
	}
	word->freq = word->freq + 1;

	printf("\tfound_word [%s] %d\n", word->word, word->freq);
}

void *count_words(void *argin) {
	thread_info_t *arg = (thread_info_t *) argin;
	word_bag_t **head = &arg->head;

	char *buf = arg->buf;
	printf("sentence [%s] %d\n", buf, sentence);
	sentence++;

	//starts with leading spaces, skip them
	int i;
	for (i = 0; i <= strlen(buf); i++) {
		if (buf[i] != ' ') break;
	}

	int start = i, end = i;
	for (; i <= strlen(buf); i++) {
		if (buf[i] == ' ') {
			if (i > 0) end = i - 1;
			count_word(head, buf, start, end);

			// we need to eat the remaining white spaces
			for (;i <= strlen(buf); i++) {
				if (buf[i] != ' ') break;
			}
			start = i;
		}
	}

	if (start != strlen(buf)) {
		count_word(head, buf, start, strlen(buf));
	}

	return NULL;
}



item_t *produce_item() {
	for (int i = 0; i < QUEUE_SIZE; i++) {
		if (queue[(next_index + i)].used == FALSE) {
			item_t *item = &queue[(next_index + i)];
			item->used = TRUE;
			num_elem++;
			next_index = (i + 1);
			return item;
		}
	}

	return NULL;
}
item_t *consume_item() {
	for (int i = 0; i < QUEUE_SIZE; i++) {
		if (queue[i].used == TRUE) {
			item_t *item = &queue[i];
			item->used = FALSE;
			num_elem--;
			return item;
		}
	}
	return NULL;
}
void *producer(void *argin) {
	FILE *f = (FILE *) argin;



	while(!feof(f)) {

		pthread_mutex_lock(&the_mutex);

		if (num_elem == QUEUE_SIZE) {
			pthread_cond_wait(&condp, &the_mutex);
		}

		item_t *item = produce_item();
		if (item != NULL) {
			int sz = fread(item->buf, 1, CHUNK_SIZE, f);
			int end = sz;
			if (!feof(f)) {
							for (end = sz; end >= 0; end--) {
								if ((item->buf[end] == ' ') || (item->buf[end] == '\t')) break;
							}
						}
			item->buf[end] = 0;
			printf("produce %d %d\n [%s]\n\n\n\n\n\n", pcount,num_elem, item->buf);
			pcount++;
		}

		if (num_elem == 1) {
			pthread_cond_signal(&condc);
		}

		pthread_mutex_unlock(&the_mutex);

	}
	producer_done = TRUE;
	printf("\n\n\n\n\n\n\n\n\n\n\n\n\n\n%d %s\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n", producer_done, "producer done");
	fclose(f);
	pthread_exit(0);



}

void *consumer(void *argin) {
	thread_info_t *arg = (thread_info_t *) argin;

	while ((producer_done == FALSE) || (num_elem > 0)){

		pthread_mutex_lock(&the_mutex);

		if ((num_elem == 0) && (producer_done != TRUE)) {
			pthread_cond_wait(&condc, &the_mutex);

		}




		item_t *item = consume_item();

		strcpy(arg->buf,item->buf);
		arg->hasrun = TRUE;
		count_words(argin);

		if (item != NULL) {
			printf("consumer%d %s %d\n [%s]\n\n\n\n\n\n",arg->tid,"ELEMENTS IN QUEUE: ",num_elem, arg->buf);

		}


		if (num_elem == QUEUE_SIZE - 1) {
			pthread_cond_signal(&condp);
		}

		pthread_mutex_unlock(&the_mutex);

	}

	endingconsumer = arg->tid;
	printf("\n\n%d\n\n", endingconsumer);
	arg->hasrun = TRUE;
	pthread_exit(0);


	return NULL;



}
void merge() {
	word_bag_t *head = NULL;

	for (int tid = 0; tid < N_THREADS; tid++) {
		// iterate to each thread's list
		for (word_bag_t *t = thread_info[tid].head; t != NULL; t = t->next) {
			word_bag_t *word_ptr = find_word(head, t->word);
			if (word_ptr == NULL) {
				word_ptr = new_word(&head, t->word);
			}
			word_ptr->freq = word_ptr->freq + t->freq;
		}
	}

	printf("\n\n\n");
	printf("******** FINAL WORD COUNT ********\n");
	for (word_bag_t *t = head; t != NULL; t = t->next) {
		 printf("final [%s] %d\n", t->word, t->freq);

	}
	printf("\n %d \n",endingconsumer);
}

int main(int argc, char **argv) {
	FILE *f = fopen(argv[1], "r");

		if (f == NULL) {
			printf("File not found\n");
			return -1;
		}

	for (int tid = 0; tid < N_THREADS; tid++) {
			thread_info[tid].head = NULL;
			thread_info[tid].tid = tid;
			thread_info[tid].hasrun = FALSE;

		}
	pthread_t pro;

	for (int i = 0; i < QUEUE_SIZE; i++) {
		queue[i].used = FALSE;
	}
   	pthread_mutex_init(&the_mutex, 0);
    pthread_cond_init(&condc, 0);
    pthread_cond_init(&condp, 0);


    for (int tid = 0; tid < N_THREADS; tid++){
    	pthread_create(&threads[tid], NULL, consumer, (void *) &thread_info[tid]);
    }

    pthread_create(&pro, NULL, producer, (void *) f);

    pthread_join(pro, 0);


    printf("ending consumer %d\n",endingconsumer);

    for (int tid = 0; tid < N_THREADS; tid++) {
			if (thread_info[tid].hasrun == TRUE){
				pthread_join(threads[tid], NULL);
			}
		}



    pthread_cond_destroy(&condc);
    pthread_cond_destroy(&condp);
    pthread_mutex_destroy(&the_mutex);






    merge();

	return 0;
}





