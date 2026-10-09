#include <stdio.h>
#include <stdlib.h>
#include <string.h>

// Definición de la clase
typedef struct {
    char nombre[50];
    int edad;
} Persona;

// Constructor
Persona* Persona_new(char *nombre, int edad) {
    Persona *p = (Persona*)malloc(sizeof(Persona));
    strcpy(p->nombre, nombre);
    p->edad = edad;
    return p;
}

// Métodos
void Persona_saludar(Persona *self) {
    printf("Hola, soy %s y tengo %d años\n", self->nombre, self->edad);
}

void Persona_envejecer(Persona *self) {
    self->edad++;
    printf("%s ahora tiene %d años\n", self->nombre, self->edad);
}

// Destructor
void Persona_delete(Persona *self) {
    free(self);
}

// Uso
int main() {
    Persona *juan = Persona_new("Juan", 25);
    Persona *maria = Persona_new("María", 30);
    
    Persona_saludar(juan);
    Persona_saludar(maria);
    
    Persona_envejecer(juan);
    Persona_envejecer(maria);
    
    Persona_delete(juan);
    Persona_delete(maria);
    
    return 0;
}
