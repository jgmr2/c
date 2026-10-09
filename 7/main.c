#include<stdlib.h>

void saludar(int n){
  for(int i=1;i<=n;i++)
  printf("hola mundo: %d \n", i);
}
int main(){
  saludar(10);
  return 0;
}
