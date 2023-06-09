-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 1.  Qual a nota (média, mínima e máxima) de cada vendedor que tiveram vendas no ano de 2017? E o percentual de pedidos avaliados com nota 5?

-- COMMAND ----------

WITH
  tb_vendedores_2017 AS (
    SELECT DISTINCT
            tb_vendedor.idVendedor AS idVendedor,
            tb_pedido.idPedido AS idPedido
    FROM silver.olist.vendedor AS tb_vendedor
    INNER JOIN silver.olist.item_pedido AS tb_item
    ON tb_vendedor.idVendedor = tb_item.idVendedor
    INNER JOIN silver.olist.pedido AS tb_pedido
    ON tb_item.idPedido = tb_pedido.idPedido
    WHERE date_trunc('YEAR', tb_pedido.dtAprovado) = '2017'
    ORDER BY idVendedor DESC
  ),

  tb_avaliacoes_5 AS (
    SELECT  tb_vendedor.idVendedor AS idVendedor,
            count(tb_avaliacao.idPedido) AS qtdValorNota5
    FROM silver.olist.vendedor AS tb_vendedor
    INNER JOIN silver.olist.item_pedido AS tb_item
    ON tb_vendedor.idVendedor = tb_item.idVendedor
    INNER JOIN silver.olist.avaliacao_pedido AS tb_avaliacao
    ON tb_item.idPedido = tb_avaliacao.idPedido
    WHERE vlNota = 5
    GROUP BY tb_vendedor.idVendedor
  ),

  tb_avaliacoes_geral AS (
    SELECT  tb_vendedor.idVendedor AS idVendedor,
            count(tb_avaliacao.idPedido) AS qtdValorNota
    FROM silver.olist.vendedor AS tb_vendedor
    INNER JOIN silver.olist.item_pedido AS tb_item
    ON tb_vendedor.idVendedor = tb_item.idVendedor
    INNER JOIN silver.olist.avaliacao_pedido AS tb_avaliacao
    ON tb_item.idPedido = tb_avaliacao.idPedido
    GROUP BY tb_vendedor.idVendedor
  ),

  tb_avaliacoes_percentual_5 AS (
    SELECT  tb_avaliacoes_geral.idVendedor,
            ifnull(round(100 * tb_avaliacoes_5.qtdValorNota5 / tb_avaliacoes_geral.qtdValorNota, 1), 0) AS percentualAcima5
    FROM tb_avaliacoes_geral
    LEFT JOIN tb_avaliacoes_5
    ON tb_avaliacoes_geral.idVendedor = tb_avaliacoes_5.idVendedor
  )

SELECT  tb_vendedores_2017.idVendedor,
        round(avg(tb_avaliacao.vlNota), 1) AS notaMedia,
        min(tb_avaliacao.vlNota) AS notaMin,
        max(tb_avaliacao.vlNota) AS notaMax,
        tb_avaliacoes_percentual_5.percentualAcima5 AS percentualAcima5
FROM tb_vendedores_2017
INNER JOIN silver.olist.avaliacao_pedido AS tb_avaliacao
ON tb_vendedores_2017.idPedido = tb_avaliacao.idPedido
LEFT JOIN tb_avaliacoes_percentual_5
ON tb_avaliacoes_percentual_5.idVendedor = tb_vendedores_2017.idVendedor
GROUP BY tb_vendedores_2017.idVendedor, tb_avaliacoes_percentual_5.percentualAcima5
ORDER BY notaMedia DESC



-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 2. Calcule o valor do pedido médio, o valor do pedido mais caro e mais barato de cada vendedor que realizaram vendas entre 2017-01-01 e 2017-06-30.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 3. Calcule a quantidade de pedidos por meio de pagamento que cada vendedor teve em seus pedidos entre 2017-01-01 e 2017-06-30.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 4. Combine a query do exercício 2 e 3 de tal forma, que cada linha seja um vendedor, e que haja colunas para cada meio de pagamento (com a quantidade de pedidos) e as colunas das estatísticas do pedido do exercício 2 (média, maior valor e menor valor).
