-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 1. Qual categoria tem mais produtos vendidos?

-- COMMAND ----------

SELECT  tb_produto.descCategoria,
        count(DISTINCT tb_item.idPedido) AS qtdVendida
FROM silver.olist.item_pedido AS tb_item
LEFT JOIN silver.olist.produto AS tb_produto
ON tb_item.idProduto = tb_produto.idProduto
GROUP BY tb_produto.descCategoria
ORDER BY qtdVendida DESC
LIMIT 5

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 2. Qual categoria tem produtos mais caros, em média? E Mediana?

-- COMMAND ----------

SELECT  tb_produto.descCategoria,
        round(avg(tb_item.vlPreco), 2) AS vlMedioPreco,
        round(median(tb_item.vlPreco), 2) AS vlMedianaPreco
FROM silver.olist.item_pedido AS tb_item
LEFT JOIN silver.olist.produto AS tb_produto
ON tb_item.idProduto = tb_produto.idProduto
GROUP BY tb_produto.descCategoria
ORDER BY vlMedioPreco DESC
LIMIT 5

-- COMMAND ----------

SELECT  tb_produto.descCategoria,
        round(avg(tb_item.vlPreco), 2) AS vlMedioPreco,
        round(median(tb_item.vlPreco), 2) AS vlMedianaPreco
FROM silver.olist.item_pedido AS tb_item
LEFT JOIN silver.olist.produto AS tb_produto
ON tb_item.idProduto = tb_produto.idProduto
GROUP BY tb_produto.descCategoria
ORDER BY vlMedianaPreco DESC
LIMIT 5

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 3. Qual categoria tem maiores fretes, em média?

-- COMMAND ----------

SELECT  tb_produto.descCategoria,
        round(avg(tb_item.vlFrete), 2) AS vlMedioFrete
FROM silver.olist.item_pedido AS tb_item
LEFT JOIN silver.olist.produto AS tb_produto
ON tb_item.idProduto = tb_produto.idProduto
GROUP BY tb_produto.descCategoria
ORDER BY vlMedioFrete DESC
LIMIT 5

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 4. Os clientes de qual estado pagam mais frete, em média?

-- COMMAND ----------

SELECT  tb_cliente.descUF,
        round(avg(tb_item.vlFrete), 2) AS vlFreteMedio
FROM silver.olist.item_pedido AS tb_item
LEFT JOIN silver.olist.pedido AS tb_pedido
ON tb_item.idPedido = tb_pedido.idPedido
LEFT JOIN silver.olist.cliente AS tb_cliente
ON tb_pedido.idCliente = tb_cliente.idCliente
GROUP BY tb_cliente.descUF
ORDER BY vlFreteMedio DESC
LIMIT 3

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 5. Clientes de quais estados avaliam melhor, em média? Proporção de 5?

-- COMMAND ----------

SELECT  tb_cliente.descUF,
        round(avg(tb_avaliacao.vlNota), 2) AS vlAvaliacao
FROM silver.olist.avaliacao_pedido AS tb_avaliacao
INNER JOIN silver.olist.pedido AS tb_pedido
ON tb_avaliacao.idPedido = tb_pedido.idPedido
INNER JOIN silver.olist.cliente AS tb_cliente
ON tb_pedido.idCliente = tb_cliente.idCliente
GROUP BY tb_cliente.descUF
ORDER BY vlAvaliacao DESC
LIMIT 5

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 6. Vendedores de quais estados têm as piores reputações?

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 7. Quais estados de clientes levam mais tempo para a mercadoria chegar?

-- COMMAND ----------

SELECT  tb_cliente.descUF,
        avg(datediff(tb_pedido.dtEntregue, tb_pedido.dtAprovado)) AS tempoEntregaMedio
FROM silver.olist.avaliacao_pedido AS tb_avaliacao
INNER JOIN silver.olist.pedido AS tb_pedido
ON tb_avaliacao.idPedido = tb_pedido.idPedido
INNER JOIN silver.olist.cliente AS tb_cliente
ON tb_pedido.idCliente = tb_cliente.idCliente
WHERE tb_pedido.dtEntregue IS NOT NULL
GROUP BY tb_cliente.descUF
ORDER BY tempoEntregaMedio DESC
LIMIT 5

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 8. Qual meio de pagamento é mais utilizado por clientes do RJ?

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 9. Qual estado sai mais ferramentas?

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 10. Qual estado tem mais compras por cliente?

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 11. Qual vendedor vendeu mais PCs?

-- COMMAND ----------

SELECT  tb_vendedor.idVendedor,
        tb_produto.descCategoria,
        count(tb_item.idPedido) AS qtdCategoriaVendida 
FROM silver.olist.item_pedido AS tb_item
LEFT JOIN silver.olist.produto AS tb_produto
ON tb_item.idProduto = tb_produto.idProduto
LEFT JOIN silver.olist.vendedor AS tb_vendedor
ON tb_vendedor.idVendedor = tb_item.idVendedor
WHERE tb_produto.descCategoria = 'pcs'
GROUP BY tb_produto.descCategoria, tb_vendedor.idVendedor
ORDER BY qtdCategoriaVendida DESC
LIMIT 5
