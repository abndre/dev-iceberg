# Projeto Pyspark

## testar arquivos no docker - exemplo

Pode-se colocar o arquivo em outros locais, só alterar o path


```
docker cp python.py spark-iceberg:/opt/spark/teste.py
docker exec -it spark-iceberg ./bin/spark-submit teste.py
```

```
docker compose up

criar um usuario, nos noteboks ele se chama teste:password, recomendo usar  o mesmo
criar dois bucket silver e  bronze e adicionar o usuario teste com acessos escrita e leitura
```
