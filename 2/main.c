#include<stdlib.h>

void condicion(int a){
  if(a){
    printf("Verdadero\n");
  }else{
    printf("Falso\n");
  }
}

int main(int argc, char *argv[]){

  if(argc < 2){
    printf("Uso %s <num>\nEjemplo %s 1\n", argv[0], argv[0]);
    return 1;
  }

  int valor = atoi(argv[1]);
  condicion(valor);
  return 0;
  
}
