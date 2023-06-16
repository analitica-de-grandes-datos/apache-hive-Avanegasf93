/* 
Pregunta
===========================================================================

Para resolver esta pregunta use el archivo `data.tsv`.

Compute la cantidad de registros por cada letra de la columna 1.
Escriba el resultado ordenado por letra. 

Apache Hive se ejecutará en modo local (sin HDFS).

Escriba el resultado a la carpeta `output` de directorio de trabajo.

        >>> Escriba su respuesta a partir de este punto <<<
*/
-- Se elimina la tabla 'tabla_de_datos' si existe
DROP TABLE IF EXISTS tabla_de_datos;

-- Se crea una nueva tabla llamada 'tabla_de_datos' con tres columnas: 'letter' de tipo STRING, 'dates' de tipo DATE y 'number' de tipo INT.
-- Los campos están delimitados por el carácter de tabulación ('\t').
CREATE TABLE tabla_de_datos (letter STRING, dates DATE, number INT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

-- Se cargan los datos del archivo local 'data.tsv' en la tabla 'tabla_de_datos',
-- sobrescribiendo los datos existentes en la tabla.
LOAD DATA LOCAL INPATH 'data.tsv' OVERWRITE INTO TABLE tabla_de_datos;

-- Se realiza una consulta para obtener el recuento de ocurrencias de cada letra ('letter') en la tabla 'tabla_de_datos'.
-- El resultado se guarda en la carpeta 'output', con los campos separados por comas (',').
INSERT OVERWRITE DIRECTORY 'output'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT letter, COUNT(letter) AS freq FROM tabla_de_datos GROUP BY letter;
