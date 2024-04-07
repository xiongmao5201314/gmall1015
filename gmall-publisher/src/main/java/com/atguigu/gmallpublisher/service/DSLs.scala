package com.atguigu.gmallpublisher.service

object DSLs {
def getSaleDetailDSL(date: String,
                     keyWord: String,
                     startPage: Int,
                     sizePerpage: Int,
                     aggField: String,
                     aggCount: Int)={
  s"""{
    |  "query": {
    |    "bool": {
    |      "filter": {
    |        "term": {
    |          "dt": "${date}"
    |        }
    |      },
    |      "must": [
    |        {
    |          "match": {
    |            "sku_name": {
    |              "query": "${keyWord}"
    |              , "operator": "and"
    |            }
    |          }
    |        }
    |      ]
    |    }
    |  },
    |  "aggs": {
    |    "group_by_${aggField}": {
    |      "terms": {
    |        "field": "${aggField}",
    |        "size": ${aggCount}
    |      }
    |    }
    |  },
    |  "from": ${(startPage-1)*sizePerpage},
    |  "size": ${sizePerpage}
    |}""".stripMargin
}
}
