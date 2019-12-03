import traceback
from operations import TopOperation
from operations import JoinOperation
from operations import AggregationOperation
from operations import FormulaOperation
from operations import FilterOperation
from connectors import DBFSConnector
from connectors import CosmosDBConnector
from datatransformations import TranformationsMainFlow
from automl import tpot_execution
from core import PipelineNotification
import json

PipelineNotification.PipelineNotification().started_notification('5db2b88f7267f58cb8bf3996','567a95c8ca676c1d07d5e3e7','http://104.40.91.74:3200/pipeline/notify')
Movie_Recc_dbfs = DBFSConnector.DBFSConnector.fetch([], {}, "5db2b88f7267f58cb8bf3996", spark, "{'url': '/Demo/Marketing/MovieRatings (2).csv', 'file_type': 'Delimeted', 'dbfs_token': 'dapi743e2d3cc92a32916f8c2fa9bd7d0606', 'dbfs_domain': 'westus.azuredatabricks.net', 'delimiter': ',', 'is_header': 'Use Header Line'}")
PipelineNotification.PipelineNotification().completed_notification('5db2b88f7267f58cb8bf3996','567a95c8ca676c1d07d5e3e7','http://104.40.91.74:3200/pipeline/notify')
PipelineNotification.PipelineNotification().started_notification('5db2b88f7267f58cb8bf3997','567a95c8ca676c1d07d5e3e7','http://104.40.91.74:3200/pipeline/notify')
Movie_Recc_FE = TranformationsMainFlow.TramformationMain.run(["5db2b88f7267f58cb8bf3996"],{"5db2b88f7267f58cb8bf3996": Movie_Recc_dbfs}, "5db2b88f7267f58cb8bf3997", spark,json.dumps( {"FE": [{"feature": "UserId", "selected": "True", "transformation": "", "type": "numeric", "stats": {"mean": "462.89", "count": "1000", "missing": "0", "stddev": "256.92", "min": "6", "max": "938"}}, {"feature": "MovieId", "selected": "True", "transformation": "", "type": "numeric", "stats": {"mean": "454.19", "count": "1000", "missing": "0", "stddev": "348.0", "min": "1", "max": "1669"}}, {"feature": "Rating", "selected": "True", "transformation": "", "type": "real", "stats": {"mean": "3.48", "count": "1000", "missing": "0", "stddev": "1.11", "min": "1.0", "max": "5.0"}}, {"feature": "Timestamp", "selected": "True", "transformationsData": {"feature_label": "Timestamp"}, "transformation": "Extract Date", "type": "date", "stats": {"mean": "", "count": "", "missing": "0", "stddev": "", "min": "", "max": ""}}, {"feature": "AvgRating", "selected": "True", "transformation": "", "type": "real", "stats": {"mean": "3.49", "count": "1000", "missing": "0", "stddev": "0.43", "min": "1.8", "max": "4.5"}}, {"feature": "Timestamp_dayofmonth", "selected": "True", "transformation": "", "type": "numeric", "stats": {"mean": "16.26", "count": "1000", "missing": "0", "stddev": "8.6", "min": "1", "max": "31"}}, {"feature": "Timestamp_month", "selected": "True", "transformation": "", "type": "numeric", "stats": {"mean": "6.96", "count": "1000", "missing": "0", "stddev": "4.35", "min": "1", "max": "12"}}, {"feature": "Timestamp_year", "selected": "True", "transformation": "", "type": "numeric", "stats": {"mean": "1997.45", "count": "1000", "missing": "0", "stddev": "0.5", "min": "1997", "max": "1998"}}]}))
PipelineNotification.PipelineNotification().completed_notification('5db2b88f7267f58cb8bf3997','567a95c8ca676c1d07d5e3e7','http://104.40.91.74:3200/pipeline/notify')
PipelineNotification.PipelineNotification().started_notification('5db2b88f7267f58cb8bf3998','567a95c8ca676c1d07d5e3e7','http://104.40.91.74:3200/pipeline/notify')
Movie_Recommendation_AutoML = tpot_execution.Tpot_execution.run(["5db2b88f7267f58cb8bf3997"],{"5db2b88f7267f58cb8bf3997": Movie_Recc_FE}, "5db2b88f7267f58cb8bf3998", spark,json.dumps( {"model_type": "regression", "label": "AvgRating", "run_id": "", "features": ["UserId", "MovieId", "Rating", "Timestamp", "Timestamp_dayofmonth", "Timestamp_month", "Timestamp_year"], "percentage": "100", "executionTime": 30, "sampling": "0", "sampling_value": "", "model_id": "5de0a9031bfdaec91f2351f1", "ProjectName": "Retail Scenarios", "PipelineName": "MoviePredictionSystem", "userid": "567a95c8ca676c1d07d5e3e7", "url_ResultView": "http://104.40.91.74:3200", "experiment_id": "895518857185768"}))
PipelineNotification.PipelineNotification().completed_notification('5db2b88f7267f58cb8bf3998','567a95c8ca676c1d07d5e3e7','http://104.40.91.74:3200/pipeline/notify')

