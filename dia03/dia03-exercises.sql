-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 1. Qual pedido com maior valor de frete? E o menor?

-- COMMAND ----------

SELECT  *
FROM silver.olist.item_pedido
WHERE vlFrete = (
  SELECT MAX(vlFrete)
  FROM silver.olist.item_pedido
)

-- COMMAND ----------

SELECT  *
FROM silver.olist.item_pedido
WHERE vlFrete = (
  SELECT MIN(vlFrete)
  FROM silver.olist.item_pedido
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 2. Qual vendedor tem mais pedidos?

-- COMMAND ----------

SELECT
  DISTINCT idVendedor,
  COUNT(DISTINCT idPedido) AS qtdPedido
FROM silver.olist.item_pedido
GROUP BY idVendedor
ORDER BY qtdPedido DESC
LIMIT 1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 3. Qual vendedor tem mais itens vendidos? E o com menos?

-- COMMAND ----------

-- DBTITLE 1,Vendedor com maior quantidade de itens vendidos

SELECT  idVendedor,
        COUNT(idProduto) AS qtdVendida
FROM silver.olist.item_pedido
GROUP BY idVendedor
ORDER BY qtdVendida DESC
LIMIT 1

-- COMMAND ----------

-- DBTITLE 1,Vendedor com menor quantidade de itens vendidos
SELECT  idVendedor,
        COUNT(idProduto) AS qtdVendida
FROM silver.olist.item_pedido
GROUP BY idVendedor
ORDER BY qtdVendida ASC
LIMIT 1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 4. Qual dia tivemos mais pedidos?

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC
-- MAGIC SELECT  DATE(dtPedido) AS dataDia,
-- MAGIC         COUNT(DISTINCT idPedido) AS qtdPedidos
-- MAGIC FROM silver.olist.pedido
-- MAGIC GROUP BY DATE(dtPedido)
-- MAGIC ORDER BY COUNT(DISTINCT idPedido) DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 5. Quantos vendedores são do estado de São Paulo?

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC SELECT  descUF,
-- MAGIC         COUNT(DISTINCT idVendedor) AS qtdVendedor
-- MAGIC FROM silver.olist.vendedor
-- MAGIC WHERE descUF = 'SP'
-- MAGIC GROUP BY descUF

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 6. Quantos vendedores são de Presidente Prudente?

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC SELECT  descCidade,
-- MAGIC         COUNT(idVendedor) AS qtdVendedor
-- MAGIC FROM silver.olist.vendedor
-- MAGIC WHERE LOWER(descCidade) = 'presidente prudente'
-- MAGIC GROUP BY descCidade

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 7. Quantos clientes são do estado do Rio de Janeiro?

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC
-- MAGIC SELECT  descUF,
-- MAGIC         count(idCliente) AS qtdCliente
-- MAGIC FROM silver.olist.cliente
-- MAGIC WHERE descUF = 'RJ'
-- MAGIC GROUP BY descUF

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 8. Quantos produtos são de construção?

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC
-- MAGIC SELECT  descCategoria,
-- MAGIC         count(idProduto) AS qtdProduto
-- MAGIC FROM silver.olist.produto
-- MAGIC WHERE descCategoria LIKE '%construcao%'
-- MAGIC GROUP BY descCategoria
-- MAGIC ORDER BY qtdProduto DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 9. Qual o valor médio de um pedido? E do frete?

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC SELECT round(avg(vlPagamento), 2) AS valor_medio_pedido
-- MAGIC FROM silver.olist.pagamento_pedido

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC SELECT round(avg(vlFrete), 2) AS valor_medio_frete
-- MAGIC FROM silver.olist.item_pedido

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 10. Em média os pedidos são de quantas parcelas de cartão? E o valor médio por parcela?

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC SELECT  round(avg(nrParcelas), 0) AS nrParcelasMedia,
-- MAGIC         round(avg(vlPagamento/nrParcelas), 2) AS vlMedioParcela
-- MAGIC FROM silver.olist.pagamento_pedido

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 11. Quanto tempo em média demora para um pedido chegar depois de aprovado?

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC SELECT  round(avg(datediff(dtEntregue, dtAprovado)), 0) AS avg_date_dif
-- MAGIC FROM silver.olist.pedido
-- MAGIC WHERE dtEntregue IS NOT NULL

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 12. Qual estado tem mais vendedores?

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC SELECT  descUF,
-- MAGIC         COUNT(idVendedor) AS qtdVendedor
-- MAGIC FROM silver.olist.vendedor
-- MAGIC -- WHERE descUF = 'SP'
-- MAGIC GROUP BY descUF
-- MAGIC ORDER BY qtdVendedor DESC
-- MAGIC LIMIT 3

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 13. Qual cidade tem mais clientes?

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC SELECT  descCidade,
-- MAGIC         count(idCliente) AS qtdCliente
-- MAGIC FROM silver.olist.cliente
-- MAGIC GROUP BY descCidade
-- MAGIC ORDER BY qtdCliente DESC
-- MAGIC LIMIT 3

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 14. Qual categoria tem mais itens?

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC SELECT  descCategoria,
-- MAGIC         count(DISTINCT idProduto) AS qtdProduto
-- MAGIC FROM silver.olist.produto
-- MAGIC -- WHERE descCategoria LIKE '%construcao%'
-- MAGIC GROUP BY descCategoria
-- MAGIC ORDER BY qtdProduto DESC
-- MAGIC LIMIT 3

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 15. Qual categoria tem maior peso médio de produto?

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC SELECT  descCategoria,
-- MAGIC         round(avg(vlPesoGramas)) AS vlPesoMedioGramas
-- MAGIC FROM silver.olist.produto
-- MAGIC GROUP BY descCategoria
-- MAGIC ORDER BY vlPesoMedioGramas DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 16. Qual a série histórica de pedidos por dia? E receita?

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC SELECT  date(tb_pedido.dtPedido) AS dataPedido,
-- MAGIC         count(DISTINCT tb_pedido.idPedido) AS qtdPedido,
-- MAGIC         round(sum(tb_pagamento.vlPagamento), 2) AS receita
-- MAGIC FROM silver.olist.pedido AS tb_pedido
-- MAGIC LEFT JOIN silver.olist.pagamento_pedido AS tb_pagamento
-- MAGIC ON tb_pedido.idPedido = tb_pagamento.idPedido
-- MAGIC GROUP BY date(tb_pedido.dtPedido)
-- MAGIC ORDER BY dataPedido DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 17. Qual produto é o campeão de vendas? Em receita? Em quantidade?

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC
-- MAGIC SELECT *
-- MAGIC FROM silver.olist.produto

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC
-- MAGIC SELECT  
-- MAGIC         item_pedido.idProduto,
-- MAGIC         produto.descCategoria,
-- MAGIC         count(DISTINCT idPedido) AS qtdVendas,
-- MAGIC         round(sum(item_pedido.vlPreco), 2) AS receita
-- MAGIC FROM silver.olist.item_pedido
-- MAGIC LEFT JOIN silver.olist.produto
-- MAGIC ON item_pedido.idProduto = produto.idProduto
-- MAGIC GROUP BY item_pedido.idProduto, produto.descCategoria
-- MAGIC ORDER BY receita DESC
-- MAGIC LIMIT 3
