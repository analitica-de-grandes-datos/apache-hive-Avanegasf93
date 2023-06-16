/*

Pregunta
===========================================================================

Escriba una consulta que retorne la primera columna, la cantidad de 
elementos en la columna 2 y la cantidad de elementos en la columna 3

Apache Hive se ejecutará en modo local (sin HDFS).

Escriba el resultado a la carpeta `output` de directorio de trabajo.

*/

DROP TABLE IF EXISTS t0;
CREATE TABLE t0 (
    c1 STRING,
    c2 ARRAY<CHAR(1)>, 
    c3 MAP<STRING, INT>
    )
    ROW FORMAT DELIMITED 
        FIELDS TERMINATED BY '\t'
        COLLECTION ITEMS TERMINATED BY ','
        MAP KEYS TERMINATED BY '#'
        LINES TERMINATED BY '\n';
LOAD DATA LOCAL INPATH 'data.tsv' INTO TABLE t0;

/*
    >>> Escriba su respuesta a partir de este punto <<<
*/
-- Guarda los resultados de la consulta en un directorio local llamado "output"
INSERT OVERWRITE LOCAL DIRECTORY 'output'

-- Establece el formato de fila como delimitado
ROW FORMAT DELIMITED 

-- Establece que los campos estén separados por coma
FIELDS TERMINATED BY ','

-- Establece que los elementos de colección estén separados por dos puntos
COLLECTION ITEMS TERMINATED BY ':'

-- Establece que las claves del mapa estén separadas por #
MAP KEYS TERMINATED BY '#'

-- Establece que cada línea esté terminada por un salto de línea
LINES TERMINATED BY '\n'

-- Realiza la consulta y selecciona las siguientes columnas
SELECT c1, size(c2), size(map_values(c3))
FROM t0;

