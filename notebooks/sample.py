from pdp_normalization.rules.rules import Rules
from pdp_normalization.normalization.normalizations import NormalizationRule
import logging, json
from pyspark.sql import Row
from pdp_normalization.common import logger
from delta_export.delta_postgresql_export import DeltaPostgreqlExport
from pdp_normalization.services.sceret_data_manager import get_secret

# Important step to avoid unwanted logs
logging.basicConfig(level=logging.INFO)
logging.getLogger("py4j.java_gateway").setLevel(logging.ERROR)

# %%
# Create sample dataframe
df = spark.createDataFrame(
    [
        Row(company="BisonPipeline", Utilization=0, ScheduledVolume=10, ActualCapacity=13),
        Row(company="BisonPipeline", Utilization=0, ScheduledVolume=12, ActualCapacity=0),
        Row(company="BisonPipeline", Utilization=0, ScheduledVolume=13, ActualCapacity=14)
    ]
)

# Sample normalization rule
rules_json = {
  "Normalization":[
    {
      "RuleName": "Create new columns with constants",
      "Filters": [],
      "Actions": [
        {
          "Type": "add_literals",
          "Columns": {
            "LocationType": "D",
            "InteruptableFlow": "Y"
          }
        }
      ],
      "Select": { }
    },
    {
      "RuleName": "Mathematical operation with formula Utilization = (ScheduledVolume/ActualCapacity) WHERE ActualCapacity is a positive value",
      "Filters": [],
      "Actions": [
        {
          "Type": "do_math",
          "LhsColumn": "ScheduledVolume",
          "RhsColumn": "ActualCapacity",
          "Operation": "divide",
          "Column": "Utilization",
          "DataType": "double",
          "ConditionalSelect": [
              {
                  "Column": "ActualCapacity",
                  "Condition": "greater_than",
                  "Values": 0
              }
          ]
        }
      ],
      "Select": { }
    }
  ]
}

# %%
# Load normalization rules
def load_normalization(rules_json: json):
    normalization_obj = []
    for i in rules_json["Normalization"]:
        normalization_obj.append(NormalizationRule(i))
    return normalization_obj

# %%
# Normalized df
def get_normalized_df(df, rules):
    # load normalization rules
    normalization_rules = load_normalization(rules)
    normalization_df = df
    # apply normalization rules
    for normalization_rule in normalization_rules:
        logger.info(normalization_rule.ruleName)
        normalization_df = normalization_rule.execute(normalization_df)
    return normalization_df

# %%
# Output
normalization_df = get_normalized_df(df, rules_json)
display(normalization_df)
