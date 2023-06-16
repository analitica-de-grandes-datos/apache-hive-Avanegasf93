/* 
Pregunta
===========================================================================

Escriba una consulta que retorne los valores únicos de la columna `t0.c5` 
(ordenados). 

Apache Hive se ejecutará en modo local (sin HDFS).

Escriba el resultado a la carpeta `output` de directorio de trabajo.
*/


DROP TABLE IF EXISTS tbl0;
CREATE TABLE tbl0 (
    c1 INT,
    c2 STRING,
    c3 INT,
    c4 DATE,
    c5 ARRAY<CHAR(1)>, 
    c6 MAP<STRING, INT>
)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ','
COLLECTION ITEMS TERMINATED BY ':'
MAP KEYS TERMINATED BY '#'
LINES TERMINATED BY '\n';
LOAD DATA LOCAL INPATH 'data0.csv' INTO TABLE tbl0;

DROP TABLE IF EXISTS tbl1;
CREATE TABLE tbl1 (
    c1 INT,
    c2 INT,
    c3 STRING,
    c4 MAP<STRING, INT>
)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ','
COLLECTION ITEMS TERMINATED BY ':'
MAP KEYS TERMINATED BY '#'
LINES TERMINATED BY '\n';
LOAD DATA LOCAL INPATH 'data1.csv' INTO TABLE tbl1;

/*
    >>> Escriba su respuesta a partir de este punto <<<
*/

-- Se ejecuta una consulta para obtener los valores distintos de la primera posición de la columna 'c5' de la tabla 'tbl0'.
-- Los valores se guardan en la carpeta 'output' en formato CSV, ordenados por 'col_5'.

INSERT OVERWRITE LOCAL DIRECTORY 'output'
-- Se especifica que los resultados se guardarán en la carpeta 'output' del directorio de trabajo en modo local.

ROW FORMAT DELIMITED 
-- Se establece el formato de las filas del archivo de salida.

FIELDS TERMINATED BY ','
-- Se establece que los campos se delimitarán por ',' en el archivo de salida.

COLLECTION ITEMS TERMINATED BY ','
-- Se establece que los elementos de las colecciones se delimitarán por ',' en el archivo de salida.

MAP KEYS TERMINATED BY '#'
-- Se establece que las claves de los mapas se delimitarán por '#' en el archivo de salida.

LINES TERMINATED BY '\n'
-- Se establece que las líneas se terminarán con '\n' en el archivo de salida.

SELECT DISTINCT c5[0] AS col_5
-- Se selecciona la primera posición de la columna 'c5' como 'col_5' y se aplica DISTINCT para obtener los valores distintos.

FROM tbl0
-- Se especifica la tabla de origen 'tbl0'.

SORT BY col_5;
-- Se ordenan los resultados por la columna 'col_5' en orden ascendente.
