#include<stdio.h>

void x(){
  printf("hola mundo\n");
}

int main(){
 void (*function)();
 function = &x;
 function();
 return 0; 
}

