#include <stdlib.h>
#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <semaphore.h>
#include <unistd.h>
#include <regex.h>
#include <errno.h>
#include <assert.h>

//размер строки
#define BUFF_SIZE 1024
#define NUMBER_OF_ARGUMENTS 6

//для работы с bool
#define true 1
#define false 0
typedef unsigned short bool;

/*-------------Создание log файла и функции работы с ним-------*/
static int d_logFile = -1;

void createLogFile(const char* file) {
  d_logFile = open(file,  O_WRONLY | O_CREAT,S_IWUSR |
                  S_IRUSR | S_IWGRP | S_IRGRP | S_IROTH);
  if (d_logFile == -1) {
    printf("Не удалось создать log файл. Приложение будет закрыто.");
    abort();
  }
}

void addMessage(const char* message) {
  assert(d_logFile != -1);
  write(d_logFile, message, strlen(message));
  write(d_logFile, "\n", 1);
}

void printErrno() {
  char* errorMessage = strerror(errno);
  addMessage(errorMessage);
}

void closeLogFile() {
  close(d_logFile);
}

//структура для хранения регулярного выражения
struct regularExpression {
  regex_t preg;
  char* pattern;
  int err;
  int regerr;
  regmatch_t pm;
};

struct regularExpression regExp;


/*------------- ОПИСАНИЕ ОЧЕРЕДИ --------------*/

//ячейка очереди
struct node {
  char* data;
  struct node* next;
};
//указатели на начало и конец очереди
struct queue {
  struct node* tail;
  struct node* head;
  int size;
};

//добавление в очередь
void push(struct queue* que, char* str) {
  que->size++;
  if (que->tail == NULL) {
    que->tail = malloc(sizeof(struct node));
    que->tail->data = malloc(sizeof(char) * BUFF_SIZE);
    strcpy(que->tail->data, str);
    que->tail->next = NULL;
    que->head = que->tail;
  }
  else {
    struct node* new = malloc(sizeof(struct node));
    new->data = malloc(sizeof(char) * BUFF_SIZE);
    strcpy(new->data, str);
    new->next = NULL;
    que->tail->next = new;
    que->tail = new;
  }
}

//извелечение из очереди
void pop(struct queue* que, char* str) {
  que->size--;
  strcpy(str,que->head->data);
  if (que->head->next == NULL)
    que->head = que->tail = NULL;
  else
    que->head = que->head->next;
}

//просмотр что в начале очереди
void front(struct queue* job, char* str) {
  strcpy(str, job->head->data);
}

//добавление в очередь пути к файлу
void* enqueueJob(void* args);

//поиск подстроки в csv файле
void* findSubstring(void* args);

//структура для передачи в функцию
struct arg {
  struct queue job;   //очередь
  char* path;         //путь к файлу
  char* subString;    //подстрока для поиска
  bool isRecursive;   //флаг проверки
  bool* isWorking;    //массив состояния тредов
};

int threadNumber;

//объявляем мьютекс и семафор
pthread_mutex_t mutexPush = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mutexPop = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mutexPrint = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mutexGetThread = PTHREAD_MUTEX_INITIALIZER;

//проверка файла на формат .csv
int check_csv(char str[]) {
	if (strlen(str) > 3 && str[strlen(str) - 1] == 'v' && str[strlen(str) - 2] == 's' && str[strlen(str) - 3] == 'c' && str[strlen(str) - 4] == '.')
		return true;
	else
		return false;
}

//проверка флага на поиск в подкаталогах
bool checkRecursive(const char* str) {
   return str[0] == 'r' ? true: false;
}

//получение количества потоков
int getNumberOfThreads(const char* argv) {
  int numberOfThreads = atoi(argv);

  if (numberOfThreads < 1) {
    char* error = "Количество потоков должно быть больше 0";

    //добавление ошибки
    addMessage(error);
    printf("%s\n", error);
    exit(0);
  }

  return numberOfThreads;
}

//создание массива рабочих тредов
bool* createArrayOfWorkingThreads(int numberOfThreads) {
  bool* isWorking = malloc(sizeof(bool) * numberOfThreads);

  for (size_t i = 0; i < numberOfThreads; i++)
    isWorking[i] = false;

  return isWorking;
}

int howManyThreadsWorks(bool* isWorking, int numberOfThreads) {
  int count = 0;

  for (size_t i = 0; i < numberOfThreads; ++i)
    if (isWorking[i]) count++;

    return count;
}

int getFreeThread(bool* isWorking, int numberOfThreads) {

  for (size_t i = 0; i < numberOfThreads; i++)
    if (isWorking[i] == false)
     return i;

  return 0;
}

void setCreatedThreads(bool* createdThreads, int numberOfThreads) {
  for (size_t i = 0; i < numberOfThreads; i++)
    createdThreads[i] = false;
}

/************************************************************************/
/*______________________________ int main ______________________________*/
/************************************************************************/
int main(int argc, char* argv[]) {
  //создание log файла
  createLogFile("logs.log");

  if (argc < NUMBER_OF_ARGUMENTS) {
    char* error = "Недостаточное количество параметров для запуска программы: Введите параметы в форме [папка_для_поиска] [шаблон_файла] [строка_для_поиска] [количество_потоков] [r для_поиска_в_подкаталогах]";

    //добавляем ошибку
    addMessage(error);
    printf("%s\n", error);
    exit(0);
  }

	//создаем структуру из очереди
  struct arg* info = (struct arg*)malloc(sizeof(struct arg));
    info->job = (struct queue){NULL, NULL, 0};
    info->path = argv[1];      //путь к папке
    info->subString = argv[3]; //строка для поиска
    info->isRecursive = checkRecursive(argv[5]);  //флаг проверки в подкаталогах
    info->isWorking = createArrayOfWorkingThreads(getNumberOfThreads(argv[4]));

  //инициализируем регулярное выражение
  regExp.pattern = argv[2];
  regExp.err = regcomp(&regExp.preg, regExp.pattern, REG_EXTENDED);
  if (regExp.err != 0) {
    char buff[BUFF_SIZE];
    regerror(regExp.err, &regExp.preg, buff, sizeof(buff));

    //добавляем ошибку в log файл
    addMessage(buff);
    printf("%s\n", buff);
  }

  //задаем количество потоков
 int numberOfThreads = getNumberOfThreads(argv[4]);
 pthread_t thread[numberOfThreads];

 //массив созданных тредов
 bool createdThreads[numberOfThreads];
 setCreatedThreads(createdThreads, numberOfThreads);

  enqueueJob((void*)info);

  int i = 0;

  //обрабатываем многопоточно файлы в очереди
  while(info->job.size) {

    //проверяем наличие свободных тредов
    if (howManyThreadsWorks(info->isWorking, numberOfThreads) < numberOfThreads) {

      //находим рабочий тред и запускаем его
      pthread_mutex_lock(&mutexGetThread);

      threadNumber = getFreeThread(info->isWorking, numberOfThreads);
      info->isWorking[threadNumber] = true;
      createdThreads[threadNumber] = true;

      pthread_create(&thread[threadNumber], NULL, findSubstring, (void*)info);
    }

    //ждем 1мс и проверяем освободились ли потоки
    sleep(0.001);
  }

  //ожидаем завершения всех потоков
  for (int i = 0; i < numberOfThreads; i++)
    if (createdThreads[i])
      pthread_join(thread[i], NULL);

  //закрывем лог файл
  closeLogFile();
  free(info->isWorking);
}
/**********************************************************************/

/*------------   добавления подходящего файла в очередь  -------------*/
void* enqueueJob(void* args) {
  struct arg* info = (struct arg*)args;

  //create directory
  DIR *dir;
	struct dirent *entry;
	struct stat bufInfo;
	char bufStr[BUFF_SIZE];

	dir = opendir(info->path);

  //На эту строчку потрачено более 2-х часов :)
  char* currentPath = info->path;

	while (entry = readdir(dir)) {bool mainIsWorking = true;
		if (!strcmp(entry->d_name,".") || !strcmp(entry->d_name,".."))
        		continue;

		strcpy(bufStr, info->path);
		strcat(bufStr, "/");
		strcat(bufStr, entry->d_name);

		stat(bufStr, &bufInfo);

    //добавляем путь к файлу в очередь, если он соответствует шаблону
		if (!regexec(&regExp.preg, entry->d_name, 0, &regExp.pm, 0)) {
      pthread_mutex_lock(&mutexPush);

         //добавляем путь к csv файлу в очередь
			   push(&info->job, bufStr);

      pthread_mutex_unlock(&mutexPush);
    }

    //поиск в подкаталогах
		if (info->isRecursive && S_ISDIR(bufInfo.st_mode)) {
      info->path = bufStr;
			enqueueJob(args);
    }

    //И на эту строчку тоже :)
    info->path = currentPath;
	}

	closedir(dir);
}

/*--------  реализация поиска подстроки в файле ----------*/
void* findSubstring(void* args) {

  //запоминаем номер текущего треда
  int currentThread = threadNumber;
  pthread_mutex_unlock(&mutexGetThread);

  struct arg* info = (struct arg*)args;

  //извлекаем строку из очереди
  pthread_mutex_lock(&mutexPrint);
    char* fileName = malloc(sizeof(char) * BUFF_SIZE);
    pop(&info->job, fileName);
  pthread_mutex_unlock(&mutexPrint);


  /*---- вычисляем префикс-функцию --------*/
  int n = strlen(info->subString);
  int* pi = malloc(sizeof(int) * n);
  pi[0] = 0;

  for (int i = 1; i < n; ++i) {
    int j = pi[i - 1];
    while (j > 0 && info->subString[i] != info->subString[j])
      j = pi[j - 1];
    if (info->subString[i] == info->subString[j]) j++;
    pi[i] = j;
  }

  /*------ Ищем подстроку в файле по алгоритму КМП ------*/
  FILE* file;
  file = fopen(fileName, "r");

  //проверка доступности файла
  if (file == NULL) {
    char* error = "Не удалось открыть файл ";
    strcat(error, fileName);

    //добавляем ошибку
    addMessage(error);
    printf("%s\n", error);
  }

  char *string = info->subString;
  int bool = 0;
  int current_pi = 0;

  while (!feof(file)) {
    char character = fgetc(file);

    while (current_pi > 0 && character != string[current_pi])
       current_pi = pi[current_pi - 1];

    if (character == string[current_pi]) current_pi++;

    //для поиска в ячейках данных csv
    if(character == ';') current_pi = 0;

    if (current_pi == strlen(string)) {
      pthread_mutex_lock(&mutexPrint);
        printf("%s  ", fileName);
        printf("%s\n", "YES");
        bool = 1;
      pthread_mutex_unlock(&mutexPrint);
        break;
    }

  }
    /*------ Ищем подстроку в файле по алгоритму КМП ------*/

      pthread_mutex_lock(&mutexPrint);
      if (bool == 0) {
          printf("%s  ", fileName);
          printf("%s\n", "NO");
      }
      pthread_mutex_unlock(&mutexPrint);

    //очищаем память и закрываем файл
    free(fileName);
    fclose(file);
  
  //текущий тред закончил работу
  info->isWorking[currentThread] = false;
}
