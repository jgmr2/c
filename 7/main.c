#include<stdio.h>

typedef struct{
  char name[50];
  int  age;
}Person;

void birthday(Person *p){
  p->age++;
}

int main(){
  Person p = {"José", 31};
  birthday(&p);
  printf("%d\n", p.age);
  return 0;
}
