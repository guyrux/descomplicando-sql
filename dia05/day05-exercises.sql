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
    FROM tb_vendedores_2017 AS tb_vendedor
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
    FROM tb_vendedores_2017 AS tb_vendedor
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
WHERE tb_vendedores_2017.idVendedor = 'ab9db4cf53b08b828b64dccaafc2d9f0'
GROUP BY tb_vendedores_2017.idVendedor, tb_avaliacoes_percentual_5.percentualAcima5
ORDER BY notaMedia DESC



-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 2. Calcule o valor do pedido médio, o valor do pedido mais caro e mais barato de cada vendedor que realizaram vendas entre 2017-01-01 e 2017-06-30.

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
    WHERE tb_pedido.dtAprovado >= '2017-01-01'
          AND tb_pedido.dtAprovado <= '2017-06-30'
    ORDER BY idVendedor DESC
  ),

  tb_pedido AS (
    SELECT  tb_pedido.idPedido AS idPedido,
            ROUND(SUM(tb_item.vlPreco), 2) AS vlPedido
    FROM silver.olist.item_pedido AS tb_item
    INNER JOIN silver.olist.pedido AS tb_pedido
    ON tb_item.idPedido = tb_pedido.idPedido
    WHERE tb_pedido.dtAprovado >= '2017-01-01'
          AND tb_pedido.dtAprovado <= '2017-06-30'
    GROUP BY tb_pedido.idPedido
  ),

  tb_vendedores_com_vl_pedidos (
    SELECT  tb_vendedores_2017.idVendedor AS idVendedor,
            ROUND(AVG(tb_pedido.vlPedido), 2) AS pedidoMedio,
            MAX(tb_pedido.vlPedido) AS pedidoCaro,
            MIN(tb_pedido.vlPedido) AS pedidoBarato
    FROM tb_vendedores_2017
    INNER JOIN tb_pedido
    ON tb_vendedores_2017.idPedido = tb_pedido.idPedido
    GROUP BY tb_vendedores_2017.idVendedor
  )

SELECT *
FROM tb_vendedores_com_vl_pedidos
where idVendedor = 'e2a1ac9bf33e5549a2a4f834e70df2f8'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 3. Calcule a quantidade de pedidos por meio de pagamento que cada vendedor teve em seus pedidos entre 2017-01-01 e 2017-06-30.

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
    WHERE tb_pedido.dtAprovado >= '2017-01-01'
          AND tb_pedido.dtAprovado <= '2017-06-30'
    ORDER BY idVendedor DESC
  ),

  tb_pagamento AS (
    SELECT  idPedido,
            descTipoPagamento,
            round(sum(vlPagamento), 2) AS vlTotal
    FROM silver.olist.pagamento_pedido
    GROUP BY  idPedido,
              descTipoPagamento
  ),

  tb_meio_pagamento AS (
      SELECT  tb_vendedores_2017.idVendedor AS idVendedor,
              tb_pagamento.descTipoPagamento,
              round(sum(tb_pagamento.vlTotal), 2) AS vlTotal
      FROM tb_vendedores_2017
      INNER JOIN tb_pagamento
      ON tb_vendedores_2017.idPedido = tb_pagamento.idPedido
      GROUP BY  tb_vendedores_2017.idVendedor,
                tb_pagamento.descTipoPagamento
  )


SELECT *
FROM tb_meio_pagamento
WHERE idVendedor = '9de4643a8dbde634fe55621059d92273'


-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 4. Combine a query do exercício 2 e 3 de tal forma, que cada linha seja um vendedor, e que haja colunas para cada meio de pagamento (com a quantidade de pedidos) e as colunas das estatísticas do pedido do exercício 2 (média, maior valor e menor valor).

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
    WHERE tb_pedido.dtAprovado >= '2017-01-01'
          AND tb_pedido.dtAprovado <= '2017-06-30'
    ORDER BY idVendedor DESC
  ),

  tb_pagamento AS (
    SELECT  idPedido,
            descTipoPagamento,
            round(sum(vlPagamento), 2) AS vlTotal
    FROM silver.olist.pagamento_pedido
    GROUP BY  idPedido,
              descTipoPagamento
  ),

  tb_meio_pagamento AS (
        SELECT  tb_vendedores_2017.idVendedor AS idVendedor,
                IF(tb_pagamento.descTipoPagamento='boleto', 1, 0) AS boleto,
                IF(tb_pagamento.descTipoPagamento='voucher', 1, 0) AS voucher,
                IF(tb_pagamento.descTipoPagamento='credit_card', 1, 0) AS credit_card,
                IF(tb_pagamento.descTipoPagamento='debit_card', 1, 0) AS debit_card
        FROM tb_vendedores_2017
        INNER JOIN tb_pagamento
        ON tb_vendedores_2017.idPedido = tb_pagamento.idPedido
        ORDER BY tb_vendedores_2017.idVendedor
  ),

  tb_meio_pagamento_sumarizado AS (
        SELECT  tb_meio_pagamento.idVendedor,
                sum(tb_meio_pagamento.boleto) AS boleto,
                sum(tb_meio_pagamento.voucher) AS voucher,
                sum(tb_meio_pagamento.credit_card) AS credit_card,
                sum(tb_meio_pagamento.debit_card) AS debit_card
        FROM tb_meio_pagamento
        GROUP BY tb_meio_pagamento.idVendedor
        ORDER BY tb_meio_pagamento.idVendedor ASC
  ),

  tb_pedido AS (
    SELECT  tb_pedido.idPedido AS idPedido,
            ROUND(SUM(tb_item.vlPreco), 2) AS vlPedido
    FROM silver.olist.item_pedido AS tb_item
    INNER JOIN silver.olist.pedido AS tb_pedido
    ON tb_item.idPedido = tb_pedido.idPedido
    WHERE tb_pedido.dtAprovado >= '2017-01-01'
          AND tb_pedido.dtAprovado <= '2017-06-30'
    GROUP BY tb_pedido.idPedido
  ),

  tb_vendedores_com_vl_pedidos AS (
    SELECT  tb_vendedores_2017.idVendedor,
            ROUND(AVG(tb_pedido.vlPedido), 2) AS pedidoMedio,
            MAX(tb_pedido.vlPedido) AS pedidoCaro,
            MIN(tb_pedido.vlPedido) AS pedidoBarato
    FROM tb_vendedores_2017
    INNER JOIN tb_pedido
    ON tb_vendedores_2017.idPedido = tb_pedido.idPedido
    GROUP BY tb_vendedores_2017.idVendedor
  )

-- SELECT  *
-- FROM tb_meio_pagamento_sumarizado

SELECT  tb_vendedores_com_vl_pedidos.idVendedor,
        tb_vendedores_com_vl_pedidos.pedidoMedio,
        tb_vendedores_com_vl_pedidos.pedidoCaro,
        tb_vendedores_com_vl_pedidos.pedidoBarato,
        tb_meio_pagamento_sumarizado.boleto,
        tb_meio_pagamento_sumarizado.voucher,
        tb_meio_pagamento_sumarizado.credit_card,
        tb_meio_pagamento_sumarizado.debit_card
FROM tb_vendedores_com_vl_pedidos
INNER JOIN tb_meio_pagamento_sumarizado
ON tb_vendedores_com_vl_pedidos.idVendedor = tb_meio_pagamento_sumarizado.idVendedor
WHERE tb_meio_pagamento_sumarizado.idVendedor = '9de4643a8dbde634fe55621059d92273'
