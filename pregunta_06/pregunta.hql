/* 

Pregunta
===========================================================================

Escriba una consulta que retorne unicamente la columna t0.c5 con sus 
elementos en mayuscula.

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
-- Guarda los resultados de la consulta en un directorio local llamado "output"
INSERT OVERWRITE LOCAL DIRECTORY 'output'

-- Establece el formato de fila como delimitado
ROW FORMAT DELIMITED 

-- Establece que los campos estén separados por coma
FIELDS TERMINATED BY ','

-- Establece que los elementos de colección estén separados por coma
COLLECTION ITEMS TERMINATED BY ','

-- Establece que las claves del mapa estén separadas por #
MAP KEYS TERMINATED BY '#'

-- Establece que cada línea esté terminada por un salto de línea
LINES TERMINATED BY '\n'

-- Realiza la consulta y aplica las siguientes transformaciones al campo c5
SELECT UPPER(CONCAT_WS(':', c5))

-- De la tabla llamada "tbl0"
FROM tbl0;

