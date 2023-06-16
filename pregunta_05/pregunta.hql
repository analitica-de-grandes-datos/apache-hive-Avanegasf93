/*

Pregunta
===========================================================================

Realice una consulta que compute la cantidad de veces que aparece cada valor 
de la columna `t0.c5`  por año.

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
-- Se eliminan las tablas existentes 'tbl0' y 'count_value' si existen previamente.
DROP TABLE IF EXISTS tbl0;
DROP TABLE IF EXISTS count_value;

-- Se crea la tabla 'tbl0' con las columnas y tipos de datos especificados.
CREATE TABLE tbl0 (
    c1 INT,
    c2 STRING,
    c3 INT,
    c4 STRING,
    c5 ARRAY<CHAR(1)>, 
    c6 MAP<STRING, INT>
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
COLLECTION ITEMS TERMINATED BY ':'
MAP KEYS TERMINATED BY '#'
LINES TERMINATED BY '\n';

-- Se carga los datos del archivo 'data0.csv' ubicado en el directorio local en la tabla 'tbl0'.
LOAD DATA LOCAL INPATH 'data0.csv' INTO TABLE tbl0;

-- Se crea la tabla 'count_value' como resultado de una consulta.
-- Se selecciona el año (extraído de la columna 'c4') como 'ano' y la letra (expandida desde el array 'c5') como 'letter'.
-- Se utiliza LATERAL VIEW explode para expandir los elementos del array.
CREATE TABLE count_value AS 
SELECT (YEAR(c4)) as ano, letter 
FROM tbl0 LATERAL VIEW explode(c5) letter_list as letter;

-- Se insertan los resultados de una consulta en la carpeta './output'.
-- Los resultados se guardan en formato CSV, los campos se delimitan por ','.
-- Se selecciona 'ano', 'letter' y el recuento 'cant' agrupados por 'ano' y 'letter', y se ordenan por 'ano' y 'letter' en orden ascendente.
INSERT OVERWRITE LOCAL DIRECTORY './output'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT ano, letter, COUNT(1) as cant 
FROM count_value 
GROUP BY ano, letter 
ORDER BY ano, letter ASC;
