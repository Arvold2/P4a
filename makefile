gcc -c  -o mapreduce.o mapreduce.c -Wall -Werror -pthread -O
gcc -o test0 test0.c mapreduce.o -Wall -Werror -pthread -O
