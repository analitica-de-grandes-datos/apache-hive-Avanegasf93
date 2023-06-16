/* 
Pregunta
===========================================================================

Para resolver esta pregunta use el archivo `data.tsv`.

Escriba una consulta que devuelva los cinco valores diferentes más pequeños 
de la tercera columna.

Apache Hive se ejecutará en modo local (sin HDFS).

Escriba el resultado a la carpeta `output` de directorio de trabajo.

        >>> Escriba su respuesta a partir de este punto <<<
*/
-- Se elimina la tabla 'tabla_3' si existe
DROP TABLE IF EXISTS tabla_3;

-- Se crea una nueva tabla llamada 'tabla_3' con tres columnas: 'letra' de tipo STRING, 'fecha' de tipo DATE y 'valor' de tipo INT.
-- Los campos están delimitados por el carácter de tabulación ('\t').
CREATE TABLE tabla_3 (
    letra STRING,
    fecha DATE,
    valor INT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

-- Se cargan los datos del archivo local 'data.tsv' en la tabla 'tabla_3'.
LOAD DATA LOCAL INPATH 'data.tsv' INTO TABLE tabla_3;

-- Se realiza una consulta para obtener los 5 valores distintos de la columna 'valor' de la tabla 'tabla_3'.
-- El resultado se guarda en la carpeta 'output' en formato CSV.
INSERT OVERWRITE DIRECTORY 'output'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT DISTINCT(valor) FROM tabla_3 ORDER BY valor LIMIT 5;


