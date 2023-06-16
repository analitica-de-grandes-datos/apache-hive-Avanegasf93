/* 
Pregunta
===========================================================================

Para resolver esta pregunta use el archivo `data.tsv`.

Construya una consulta que ordene la tabla por letra y valor (3ra columna).

Apache Hive se ejecutará en modo local (sin HDFS).

Escriba el resultado a la carpeta `output` de directorio de trabajo.

        >>> Escriba su respuesta a partir de este punto <<<
*/
-- Se elimina la tabla 'tabla_2' si existe
DROP TABLE IF EXISTS tabla_2;

-- Se crea una nueva tabla llamada 'tabla_2' con tres columnas: 'letra' de tipo STRING, 'fecha' de tipo DATE y 'valor' de tipo INT.
-- Los campos están delimitados por el carácter de tabulación ('\t').
CREATE TABLE tabla_2 (
    letra STRING,
    fecha DATE,
    valor INT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

-- Se cargan los datos del archivo local 'data.tsv' en la tabla 'tabla_2'.
LOAD DATA LOCAL INPATH 'data.tsv' INTO TABLE tabla_2;

-- Se realiza una consulta para obtener todos los registros de la tabla 'tabla_2', ordenados por 'letra', 'valor' y 'fecha'.
-- El resultado se guarda en la carpeta 'output' en formato CSV.
INSERT OVERWRITE DIRECTORY 'output'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT * FROM tabla_2 ORDER BY letra, valor, fecha;


