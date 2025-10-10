from prettytable import PrettyTable
from IPython.core.magic import register_line_cell_magic
from IPython import get_ipython
from pyspark.sql import SparkSession
import re
import sys
from argparse import ArgumentParser

# --------------------------
# Tabelas formatadas no notebook
# --------------------------
class DFTable(PrettyTable):
    def __repr__(self):
        return self.get_string()

    def _repr_html_(self):
        return self.get_html_string()


def _row_as_table(df):
    cols = df.columns
    t = DFTable()
    t.field_names = ["Column", "Value"]
    t.align = "r"
    row = df.limit(1).collect()[0].asDict()
    for col in cols:
        t.add_row([col, row[col]])
    return t


def _to_table(df, num_rows=100):
    cols = df.columns
    t = DFTable()
    t.field_names = cols
    t.align = "r"
    for row in df.limit(num_rows).collect():
        d = row.asDict()
        t.add_row([d[col] for col in cols])
    return t


# --------------------------
# Argumentos da mágica
# --------------------------
parser = ArgumentParser()
parser.add_argument("--limit", help="Number of lines to return", type=int, default=100)
parser.add_argument("--var", help="Variable name to hold the dataframe", type=str)


# --------------------------
# Função principal da mágica
# --------------------------
@register_line_cell_magic
def sql(line, cell=None):
    """
    Spark SQL magic
    Uso:
    %sql SELECT * FROM tabela
    ou
    %%sql --limit 10 --var df_result
    SELECT * FROM tabela
    """
    # Pega a SparkSession ativa (ou cria uma)
    user_ns = get_ipython().user_ns
    spark = user_ns.get("spark") or SparkSession.builder.appName("SQLMagic").getOrCreate()

    # Se for apenas linha (sem célula)
    if cell is None:
        return _to_table(spark.sql(line))

    # Se tiver célula com código SQL
    elif line:
        df = spark.sql(cell)
        args, _ = parser.parse_known_args([arg for arg in re.split(r"\s+", line) if arg])

        # Salva o DataFrame em uma variável Python se solicitado
        if args.var:
            user_ns[args.var] = df

        # Retorna resultado formatado
        if args.limit == 1:
            return _row_as_table(df)
        else:
            return _to_table(df, num_rows=args.limit)
    else:
        return _to_table(spark.sql(cell))


# --------------------------
# Integração com %load_ext
# --------------------------
def load_ipython_extension(ipython):
    """Função chamada automaticamente ao executar %load_ext spark_sql_magic"""
    print("✅ Spark SQL Magic carregado! Use %sql ou %%sql para executar consultas Spark SQL.")
