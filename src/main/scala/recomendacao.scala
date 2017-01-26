import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._

import scala.util.control.Breaks._

import java.io._

object Recomendacao {
  def main(args: Array[String]) = {
    val conf = new SparkConf()
      .setAppName("GnResende Recomendacao")
    val sc = new SparkContext(conf)

    val spark = SparkSession
      .builder()
      .appName("Recomendacao_SQL")
      .getOrCreate()

    import spark.implicits._

    val path = "/projetos/recomendacao/data/user-product_map.json"
    val userProductDF = spark.read.json(path)

    userProductDF.createOrReplaceTempView("userProduct")

    val productSalesRDD = spark.sql("""
      SELECT
        DISTINCT
          A.product_id, B.user_id, C.product_id as product_id_sim
      FROM
        (SELECT distinct product_id FROM userProduct) A,
        (SELECT user_id, product_id FROM userProduct) B,
        (SELECT user_id, product_id FROM userProduct) C
      WHERE
        B.product_id = A.product_id and 
        ((C.user_id = B.user_id) and (C.product_id != A.product_id))
      ORDER BY
        A.product_id""").collect()

    val productsSales = productSalesRDD.groupBy(x => x.getLong(0)).map(row => Row(row._1, row._2.map(_.getAs[Long]("user_id")).toList))

    val productsSimilar = productSalesRDD.groupBy(x => x.getLong(0)).map(row => Row(row._1, 
                                                  row._2.map(_.getAs[Long]("product_id_sim")).toList))

    println("\n\n Nao mais paralelizado nos workers/executors, agora somente no Driver - Apos collect")
    println("\n\n Calculando a raiz quadrada da somatoria dos quadrados para cada item e criando dois Maps para facilitar a pesquisa")

    var userDouble = 0f
    var valor: Long = 0
    var produtosUsers = collection.mutable.Map.empty[Long,collection.mutable.MutableList[Long]]
    var produtosSomaQuadradoUsers = collection.mutable.Map.empty[Long,Double]

    for(prodSales <- productsSales) {
      userDouble = 0f
      if (prodSales.getList(1).size() > 0 ) {
        if (produtosUsers.get(prodSales.getLong(0)) == None) {
          produtosUsers += prodSales.getLong(0) -> collection.mutable.MutableList(prodSales.getList(1).get(0))
        }
        var listaUsers = produtosUsers.get(prodSales.getLong(0)).get
        for( item <- 1 to prodSales.getList(1).size()-1) {
          valor = prodSales.getList(1).get(item)
          listaUsers += valor 
          userDouble += valor * valor
        }
        produtosSomaQuadradoUsers += (prodSales.getLong(0) -> Math.sqrt(userDouble))
      }
    }

    println("\n\nProducts X Similiars - Calculando a similaridade")

    var produto : Long = 0
    var produtoCompraJunto : Long = 0
    var produtosSimilaridade = collection.mutable.Map.empty[Long,collection.mutable.Map[Long,Double]]
    var somatoria : Double = 0.toDouble
    var calculo : Double = 0.toDouble

    for(prodSim <- productsSimilar) {
      produto = prodSim.getLong(0)
      var usersComprasProduto = produtosUsers.get(produto).get
      if (prodSim.getList(1).size() > 0) {
        for( item <- 0 to prodSim.getList(1).size()-1) {
          produtoCompraJunto = prodSim.getList(1).get(item)
          if (produtosUsers.get(produtoCompraJunto) != None) {
            var usersComprasProdutoCompraJunto = produtosUsers.get(produtoCompraJunto).get
            if (usersComprasProduto.length <= usersComprasProdutoCompraJunto.length) {
              for( item <- 0 to usersComprasProduto.length-1) {
                somatoria = usersComprasProduto.get(item).get * usersComprasProdutoCompraJunto.get(item).get
              }
            } else {
              for( item <- 0 to usersComprasProdutoCompraJunto.length-1) {
                somatoria = usersComprasProdutoCompraJunto.get(item).get * usersComprasProduto.get(item).get
              }
            }
            calculo = somatoria / (produtosSomaQuadradoUsers.get(produto).get * 
                                 produtosSomaQuadradoUsers.get(produtoCompraJunto).get).toDouble
  
            if (produtosSimilaridade.get(produto) == None) {
              produtosSimilaridade += produto -> collection.mutable.Map(produtoCompraJunto -> calculo)
            } else {
              produtosSimilaridade.get(produto).get += produtoCompraJunto -> calculo
            }
          }
        }
      }
    }

    val file = new File("/projetos/recomendacao/data/resultados.json")
    val bw = new BufferedWriter(new FileWriter(file))

    println("\n\nGerando resultados pós-processamento ja ordenados em arquivo no Driver\n\n")
    var qtdProdutos = 0
    var algum = false
    for((produto,prodsSims) <- produtosSimilaridade) {
      bw.write("{\n")
      val prodsSimsOrdenados = collection.immutable.ListMap(prodsSims.toSeq.sortBy(_._2):_*)
      qtdProdutos = 0
      algum = false
      breakable {
        for((prodSim,calculoSimilaridade) <- prodsSimsOrdenados) {
          if ( (!calculoSimilaridade.isInfinite) && (calculoSimilaridade > 0)) {
            if (!algum) {
              algum = true
              bw.write("  \"reference_product_id\": " + produto + ",\n")
              bw.write("  \"recommendations\": [\n")
            } 
            bw.write("    {\"product_id\": " + prodSim + ", \"similarity\": " + "%1.4f".format(calculoSimilaridade) + "}")
            qtdProdutos += 1
            if (qtdProdutos >= 5) break
            bw.write(",\n")
          }
        }
      }
      if (algum) bw.write("    ]\n")
      bw.write("},\n")
    }
    bw.close()
  }
}
