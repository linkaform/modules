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
db.serie_onts.createIndex(
  { ont_serie: 1 },
  { unique: true, name: "ont_serie_unique_index" }
)

```
ejemplo


```
linkaform_replica:PRIMARY> db.serie_onts.createIndex(
...   { ont_serie: 1 },
...   { unique: true, name: "ont_serie_unique_index" }
... )
{
	"numIndexesBefore" : 1,
	"numIndexesAfter" : 2,
	"createdCollectionAutomatically" : false,
	"commitQuorum" : "votingMembers",
	"ok" : 1,
	"$clusterTime" : {
		"clusterTime" : Timestamp(1733378763, 1),
		"signature" : {
			"hash" : BinData(0,"oOUGLK6XXtnQamCEQC4o1/RcnMs="),
			"keyId" : NumberLong("7378766531926163472")
		}
	},
	"operationTime" : Timestamp(1733378763, 1)
}
linkaform_replica:PRIMARY> db.serie_onts.getIndexes()
[
	{
		"v" : 2,
		"key" : {
			"_id" : 1
		},
		"name" : "_id_"
	},
	{
		"v" : 2,
		"key" : {
			"ont_serie" : 1
		},
		"name" : "ont_serie_unique_index",
		"unique" : true
	}
]
```
