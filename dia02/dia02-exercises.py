# Databricks notebook source
# DBTITLE 1,Dia 02 - Exercises
# MAGIC %md
# MAGIC
# MAGIC 1. Selecione todos os clientes paulistanos.
# MAGIC 2. Selecione todos os clientes paulistas.
# MAGIC 3. Selecione todos os vendedores cariocas e paulistas.
# MAGIC 4. Selecione produtos de perfumaria e bebes com altura maior que 5cm.
# MAGIC 5. Selecione os pedidos com mais de um item.
# MAGIC 6. Selecione os pedidos que o frete é mais caro que o item.
# MAGIC 7. Lista de pedidos que ainda não foram enviados.
# MAGIC 8. Lista de pedidos que foram entregues com atraso.
# MAGIC 9. Lista de pedidos que foram entregues com 2 dias de antecedência.
# MAGIC 10. Selecione os pedidos feitos em dezembro de 2017 e entregues com atraso.
# MAGIC 11. Selecione os pedidos com avaliação maior ou igual que 4.
# MAGIC 12. Selecione pedidos pagos com cartão de crédito com duas ou mais parcelas menores que R$40,00.

# COMMAND ----------

# DBTITLE 1,1. Selecione todos os clientes paulistanos.
# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM silver.olist.cliente
# MAGIC WHERE descUF = 'SP'
# MAGIC       AND descCidade = 'sao paulo'
# MAGIC LIMIT 10

# COMMAND ----------

# DBTITLE 1,2. Selecione todos os clientes paulistas.
# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM silver.olist.cliente
# MAGIC WHERE descUF = 'SP'
# MAGIC LIMIT 10

# COMMAND ----------

# DBTITLE 1,3. Selecione todos os vendedores cariocas e paulistas.
# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM silver.olist.vendedor
# MAGIC WHERE descUF IN ('SP', 'RJ')
# MAGIC LIMIT 10

# COMMAND ----------

# DBTITLE 1,4. Selecione produtos de perfumaria e bebes com altura maior que 5cm.
# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM silver.olist.produto
# MAGIC WHERE   vlComprimentoCm > 5
# MAGIC         AND descCategoria IN ('perfumaria', 'bebes')
# MAGIC LIMIT 10

# COMMAND ----------

# DBTITLE 1,5. Selecione os pedidos com mais de um item.
# MAGIC %sql
# MAGIC SELECT  idPedido,
# MAGIC         COUNT(idProduto) AS qtyProducts
# MAGIC FROM silver.olist.item_pedido
# MAGIC GROUP BY idPedido
# MAGIC HAVING COUNT(idProduto) > 1
# MAGIC LIMIT 10

# COMMAND ----------

# DBTITLE 1, 6. Selecione os pedidos que o frete é mais caro que o item.
# MAGIC %sql
# MAGIC SELECT DISTINCT idPedido
# MAGIC FROM silver.olist.item_pedido
# MAGIC WHERE vlFrete > vlPreco
# MAGIC ORDER BY idPedido

# COMMAND ----------

# DBTITLE 1,7. Lista de pedidos que ainda não foram enviados.
# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM silver.olist.pedido
# MAGIC WHERE descSituacao IN ('approved', 'invoiced', 'created')

# COMMAND ----------

# DBTITLE 1,8. Lista de pedidos que foram entregues com atraso.
# MAGIC %sql
# MAGIC SELECT * -- DISTINCT descSituacao
# MAGIC FROM silver.olist.pedido
# MAGIC WHERE descSituacao IN ('delivered')
# MAGIC       AND dtEstimativaEntrega < dtEntregue

# COMMAND ----------

# DBTITLE 1,9. Lista de pedidos que foram entregues com 2 dias de antecedência.
# MAGIC %sql
# MAGIC SELECT * -- DISTINCT descSituacao
# MAGIC FROM silver.olist.pedido
# MAGIC WHERE descSituacao IN ('delivered')
# MAGIC       AND DATE_DIFF(dtEstimativaEntrega, dtEntregue) = 2

# COMMAND ----------

# DBTITLE 1,10. Selecione os pedidos feitos em dezembro de 2017 e entregues com atraso.
# MAGIC %sql
# MAGIC SELECT * -- DISTINCT descSituacao
# MAGIC FROM silver.olist.pedido
# MAGIC WHERE MONTH(dtPedido) = 12
# MAGIC       AND YEAR(dtPedido) = 2017
# MAGIC       AND dtEntregue < dtEstimativaEntrega

# COMMAND ----------

# DBTITLE 1,11. Selecione os pedidos com avaliação maior ou igual que 4.
# MAGIC %sql
# MAGIC SELECT DISTINCT (idPedido)
# MAGIC FROM silver.olist.avaliacao_pedido
# MAGIC WHERE vlNota >= 4

# COMMAND ----------

# DBTITLE 1,12. Selecione pedidos pagos com cartão de crédito com duas ou mais parcelas menores que R$40,00.
# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM silver.olist.pagamento_pedido
# MAGIC WHERE descTipoPagamento = 'credit_card'
# MAGIC       AND nrParcelas >= 2
# MAGIC       AND vlPagamento/nrParcelas < 40
