-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 1. Quais são os Top 5 vendedores campeões de vendas de cada UF?

-- COMMAND ----------

WITH
        cte_vendedor_uf_pedidos AS (
                SELECT  
                        tb_vendedor.idVendedor AS idVendedor,
                        tb_vendedor.descUF AS descUF,
                        COUNT(DISTINCT tb_pedido.idPedido) AS qtdPedidos
                FROM silver.olist.vendedor AS tb_vendedor
                INNER JOIN silver.olist.item_pedido AS tb_pedido
                ON tb_vendedor.idVendedor = tb_pedido.idVendedor
                GROUP BY tb_vendedor.idVendedor, descUF
                ),

        cte_ranking_uf AS (
                SELECT  *,
                        row_number() OVER (PARTITION BY descUF ORDER BY qtdPedidos DESC) AS rankingUF
                FROM cte_vendedor_uf_pedidos
                ORDER BY descUF ASC, qtdPedidos DESC
        )

SELECT *
FROM cte_ranking_uf
WHERE rankingUF < 6 AND descUF='BA' -- idVendedor = '1a3df491d1c4f1589fc2b934ada68bf2' 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 2. Quais são os Top 5 vendedores campeões de vendas em cada categoria?

-- COMMAND ----------

WITH
  cte_vendedor_categoria AS (
    SELECT  
        tb_vendedor.idVendedor AS idVendedor,
        IF(tb_produto.descCategoria IS NULL, 'SEM CATEGORIA', tb_produto.descCategoria) AS descCategoria,
        COUNT(DISTINCT tb_pedido.idPedido) AS qtdPedidos
    FROM silver.olist.vendedor AS tb_vendedor
    INNER JOIN silver.olist.item_pedido AS tb_pedido
    ON tb_vendedor.idVendedor = tb_pedido.idVendedor
    INNER JOIN silver.olist.produto AS tb_produto
    ON tb_pedido.idProduto = tb_produto.idProduto
    GROUP BY tb_vendedor.idVendedor, tb_produto.descCategoria
  ),

  cte_vendedor_categoria_ranking AS (
          SELECT  *,
              row_number() OVER (PARTITION BY descCategoria ORDER BY qtdPedidos DESC) AS rankingCategoria
          FROM cte_vendedor_categoria
  )

SELECT *
FROM cte_vendedor_categoria_ranking
WHERE rankingCategoria < 6

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 3. Qual é a Top 1 categoria de cada vendedor?

-- COMMAND ----------

WITH
  cte_vendedor_categoria AS (
    SELECT  
        tb_vendedor.idVendedor AS idVendedor,
        IF(tb_produto.descCategoria IS NULL, 'SEM CATEGORIA', tb_produto.descCategoria) AS descCategoria,
        COUNT(DISTINCT tb_pedido.idPedido) AS qtdPedidos
    FROM silver.olist.vendedor AS tb_vendedor
    INNER JOIN silver.olist.item_pedido AS tb_pedido
    ON tb_vendedor.idVendedor = tb_pedido.idVendedor
    INNER JOIN silver.olist.produto AS tb_produto
    ON tb_pedido.idProduto = tb_produto.idProduto
    GROUP BY tb_vendedor.idVendedor, tb_produto.descCategoria
  ),

  cte_vendedor_top_categoria AS (
          SELECT  *,
              row_number() OVER (PARTITION BY idVendedor ORDER BY qtdPedidos DESC) AS topCategoria
          FROM cte_vendedor_categoria
  )

SELECT *
FROM cte_vendedor_top_categoria
WHERE topCategoria = 1 AND idVendedor = '1cbd50a8c52e6cf8e315c5709fab386f'
ORDER BY idVendedor

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 4. Quais são as Top 2 categorias que mais vendem para clientes de cada estado?

-- COMMAND ----------

WITH
  tb_cliente_categoria AS (
    SELECT  tb_cliente.descUF AS descUF,
            tb_produto.descCategoria AS descCategoria,
            count(tb_item.idPedidoItem) AS contagemItens
    FROM silver.olist.cliente AS tb_cliente
    INNER JOIN silver.olist.pedido AS tb_pedido
    ON tb_pedido.idCliente = tb_cliente.idCliente
    LEFT JOIN silver.olist.item_pedido AS tb_item
    ON tb_pedido.idPedido = tb_item.idPedido
    LEFT JOIN silver.olist.produto AS tb_produto
    ON tb_produto.idProduto = tb_item.idProduto
    GROUP BY descUF, descCategoria
  ),

  tb_cliente_categoria_ranking AS (
    SELECT  *,
            row_number() OVER (PARTITION BY descUF ORDER BY contagemItens DESC) AS rankingUF
    FROM tb_cliente_categoria
  )

SELECT *
FROM  tb_cliente_categoria_ranking
WHERE rankingUF < 3 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 5. Quantidade acumulada de itens vendidos por categoria ao longo do tempo.

-- COMMAND ----------

WITH
  tb_contagem_categoria AS (
  SELECT  date(tb_pedido.dtAprovado) AS diaAprovado,
          tb_produto.descCategoria AS descCategoria,
          count(tb_item.idPedidoItem) AS contagemItens
  FROM silver.olist.item_pedido AS tb_item
  INNER JOIN silver.olist.pedido AS tb_pedido
  ON tb_pedido.idPedido = tb_item.idPedido
  INNER JOIN silver.olist.produto AS tb_produto
  ON tb_item.idProduto = tb_produto.idProduto
  WHERE
        -- tb_pedido.descSituacao NOT IN ('unavailable', 'canceled')
        tb_pedido.dtAprovado IS NOT NULL
  GROUP BY diaAprovado, descCategoria
  ORDER BY diaAprovado DESC
  ),

  tb_contagem_categoria_acumulada (
    SELECT *,
           sum(contagemItens) OVER (PARTITION BY descCategoria ORDER BY diaAprovado) AS contagemAcumulada
    FROM tb_contagem_categoria
  )
  
SELECT *
FROM tb_contagem_categoria_acumulada
WHERE diaAprovado = '2018-01-01'


-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 6. Receita acumulada por categoria ao longo do tempo

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 7. PLUS: Selecione um dia de venda aleatório de cada vendedor
