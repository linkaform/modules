# Modulo de Stock de PCI

## Particularidades

### Formas


En la forma de Recpecion de material se agregaron capmos para cargar documentos de excel.
Con estos documentos se carga los materiales a recibir o ONT que son numeros de seire de los modems que reciben

Para que se puedan cargar varios modems a la vez  (mas de 10K) se hacen las escrituras dirctas a la base de datos.

Tambies se pudo para la salida de materiales hacia los contratisasa

Otra regla es que no se pueden recibir mas de 1 vez un numero de serie u ONT por eso se creo el index
para que el campo `ont_serie` sea `unique` en la coleccion llamada `seire_ont`

### Base de datos

Se crea collecion llamada `serie_onts`

Para crear el indice correr

```
db.series.createIndex(
  { ont_serie: 1 },
  { unique: true, name: "ont_serie_unique_index" }
)

```
