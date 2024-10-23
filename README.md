# Projeto Pyspark

## testar arquivos no docker - exemplo

Pode-se colocar o arquivo em outros locais, só alterar o path


```
docker cp python.py spark-iceberg:/opt/spark/teste.py
docker exec -it spark-iceberg ./bin/spark-submit teste.py
```
