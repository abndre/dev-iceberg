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

## Tutorials

### Configuring S3 for Minio 

To configure your **S3 source** for **Minio** in the Dremio UI:

1. Under *Advanced Options*, check **Enable compatibility mode (experimental)**.
2. Under *Advanced Options > Connection Properties*, add **fs.s3a.path.style.access** and set the value to **true**.
3. Under *Advanced Options > Connection Properties*, add the **fs.s3a.endpoint** property and its corresponding server endpoint value (*minio:9000* in the Datafuel ecosystem).
4. Under *Advanced Options > Connection Properties*, add the **dremio.s3.compat** and set the value to **true**.