import json
data = json.dumps({
  "query": """PREFIX gist: <https://ontologies.semanticarts.com/gist/>
PREFIX pleo: <https://ontologies.platts.com/pleo/>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>

SELECT (COALESCE(?inner_regionIRI, "") as ?regionIRI) 
(COALESCE(?inner_regionName, "") as ?regionName) ?isCountry ?countryIRI ?countryName (GROUP_CONCAT(DISTINCT ?alternativeName ; 
separator=";") AS ?altName) WHERE {{ 
    ?countryIRI a pleo:Region . 
              OPTIONAL {{?countryIRI pleo:identifyingLabel ?countryNameText .}}
              ?countryIRI skos:prefLabel|skos:altLabel|pleo:hasOfficialName|pleo:hasFormerOfficialName ?countryNameText .
              OPTIONAL {{
                ?countryIRI gist:directPartOf ?inner_regionIRI .
                ?inner_regionIRI a pleo:Region .
                ?inner_regionIRI skos:prefLabel ?inner_regionName .
              }}
              OPTIONAL {{ 
                ?countryIRI a pleo:CountryRegion . 
                BIND(1 as ?isCountry) .
              }}
              OPTIONAL {{?countryIRI skos:altLabel|pleo:hasOfficialName|pleo:hasFormerOfficialName ?alt.}}
              BIND(if(bound(?alt),?alt,'') as ?alternativeName)
              OPTIONAL {{?countryIRI skos:prefLabel ?pref.}}
              BIND(if(bound(?pref),?pref,'') as ?countryName)
}} GROUP BY ?inner_regionIRI ?inner_regionName ?isCountry ?countryIRI ?countryName order by ?countryName"""
})
    
from pyspark.sql.column import Column as cl, _to_java_column
def explode_outer(col):
    _explode_outer = sc._jvm.org.apache.spark.sql.functions.explode_outer
    return cl(_explode_outer(_to_java_column(col)))

import json
import requests
header = {"content-type": "application/json"}
ONTOLOGY_API_URL = 'https://api-dp-referencedata.platts.com/pdpapigraph/v1/sparql'
response = requests.post(ONTOLOGY_API_URL, data=data, headers = header)
ref_df = spark.read.json(sc.parallelize(json.loads(response.text)))
ref_df = ref_df.drop("_corrupt_record").where(ref_df.isCountry == 1)

display(ref_df)
print(ref_df.count())

from pyspark.sql import Row
df = spark.createDataFrame(
    [
        Row(company="BisonPipeline", Utilization=0, ScheduledVolume=10, ActualCapacity=13, Country='Bahrain'),
        Row(company="TranswesternPipeline", Utilization=1, ScheduledVolume=12, ActualCapacity=0, Country='Nation of Brunei, the Abode of Peace'),
        Row(company="NordPipeline", Utilization=2, ScheduledVolume=13, ActualCapacity=14, Country='the Gambia')
    ]
)
display(df)

from pyspark.sql.functions import col, split                                    
col_exprs = [split(col('altName'), "\;").alias('altNameSplitted')]
ref_df = ref_df.withColumn("altNameSplitted", *col_exprs)
display(ref_df)
print(ref_df.count())

ref_df = ref_df.withColumn("alternativeName", explode_outer(ref_df.altNameSplitted)).na.fill("")
display(ref_df)

from pyspark.sql.functions import lower
columns_to_drop = ["altName", "countryName", "isCountry", "regionIRI", "regionName", "altNameSplitted", "alternativeName"]
df = df.join(ref_df, (lower(df.Country) == lower(ref_df.countryName)) | (lower(df.Country) == lower(ref_df.alternativeName)), how='left') \
          .na.fill("") \
          .drop(*columns_to_drop)
display(df)