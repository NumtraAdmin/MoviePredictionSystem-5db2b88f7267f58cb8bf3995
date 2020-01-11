import traceback
import sys
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

try: 
	PipelineNotification.PipelineNotification().started_notification('5db2b88f7267f58cb8bf3996','567a95c8ca676c1d07d5e3e7','http://104.40.91.74:3200/pipeline/notify')
	Movie_Recc_dbfs = DBFSConnector.DBFSConnector.fetch([], {}, "5db2b88f7267f58cb8bf3996", spark, "{'url': '/Demo/Marketing/MovieRatings (2).csv', 'file_type': 'Delimeted', 'dbfs_token': 'dapi743e2d3cc92a32916f8c2fa9bd7d0606', 'dbfs_domain': 'westus.azuredatabricks.net', 'delimiter': ',', 'is_header': 'Use Header Line'}")

	PipelineNotification.PipelineNotification().completed_notification('5db2b88f7267f58cb8bf3996','567a95c8ca676c1d07d5e3e7','http://104.40.91.74:3200/pipeline/notify')
except Exception as ex: 
	PipelineNotification.PipelineNotification().failed_notification(ex,'5db2b88f7267f58cb8bf3996','567a95c8ca676c1d07d5e3e7','http://104.40.91.74:3200/pipeline/notify','http://104.40.91.74:3200/logs/getProductLogs')
	sys.exit(1)
try: 
	PipelineNotification.PipelineNotification().started_notification('5db2b88f7267f58cb8bf3997','567a95c8ca676c1d07d5e3e7','http://104.40.91.74:3200/pipeline/notify')
	Movie_Recc_FE = TranformationsMainFlow.TramformationMain.run(["5db2b88f7267f58cb8bf3996"],{"5db2b88f7267f58cb8bf3996": Movie_Recc_dbfs}, "5db2b88f7267f58cb8bf3997", spark,json.dumps( {"FE": [{"feature": "UserId", "transformation": "", "type": "numeric", "selected": "True", "stats": {"count": "1000", "mean": "479.21", "stddev": "256.29", "min": "2", "max": "938", "missing": "0"}}, {"feature": "MovieId", "transformation": "", "type": "numeric", "selected": "True", "stats": {"count": "1000", "mean": "442.62", "stddev": "350.78", "min": "1", "max": "1677", "missing": "0"}}, {"feature": "Rating", "transformation": "", "type": "real", "selected": "True", "stats": {"count": "1000", "mean": "3.44", "stddev": "1.14", "min": "1.0", "max": "5.0", "missing": "0"}}, {"transformationsData": {"feature_label": "Timestamp"}, "feature": "Timestamp", "transformation": "Extract Date", "type": "date", "selected": "True", "stats": {"count": "", "mean": "", "stddev": "", "min": "", "max": "", "missing": "0"}}, {"feature": "AvgRating", "transformation": "", "type": "real", "selected": "True", "stats": {"count": "1000", "mean": "3.45", "stddev": "0.45", "min": "1.8", "max": "4.5", "missing": "0"}}, {"feature": "Timestamp_dayofmonth", "transformation": "", "type": "numeric", "selected": "True", "stats": {"count": "1000", "mean": "15.89", "stddev": "8.74", "min": "1", "max": "31", "missing": "0"}}, {"feature": "Timestamp_month", "transformation": "", "type": "numeric", "selected": "True", "stats": {"count": "1000", "mean": "7.04", "stddev": "4.42", "min": "1", "max": "12", "missing": "0"}}, {"feature": "Timestamp_year", "transformation": "", "type": "numeric", "selected": "True", "stats": {"count": "1000", "mean": "1997.44", "stddev": "0.5", "min": "1997", "max": "1998", "missing": "0"}}]}))

	PipelineNotification.PipelineNotification().completed_notification('5db2b88f7267f58cb8bf3997','567a95c8ca676c1d07d5e3e7','http://104.40.91.74:3200/pipeline/notify')
except Exception as ex: 
	PipelineNotification.PipelineNotification().failed_notification(ex,'5db2b88f7267f58cb8bf3997','567a95c8ca676c1d07d5e3e7','http://104.40.91.74:3200/pipeline/notify','http://104.40.91.74:3200/logs/getProductLogs')
	sys.exit(1)
try: 
	PipelineNotification.PipelineNotification().started_notification('5db2b88f7267f58cb8bf3998','567a95c8ca676c1d07d5e3e7','http://104.40.91.74:3200/pipeline/notify')
	Movie_Recommendation_AutoML = tpot_execution.Tpot_execution.run(["5db2b88f7267f58cb8bf3997"],{"5db2b88f7267f58cb8bf3997": Movie_Recc_FE}, "5db2b88f7267f58cb8bf3998", spark,json.dumps( {"model_type": "regression", "label": "AvgRating", "run_id": "", "features": ["UserId", "MovieId", "Rating", "Timestamp", "Timestamp_dayofmonth", "Timestamp_month", "Timestamp_year"], "percentage": "70", "executionTime": 15, "sampling": "0", "sampling_value": "", "model_id": "5e16f4ec1bfdaec91f363cb6", "ProjectName": "Retail Scenarios", "PipelineName": "MoviePredictionSystem", "userid": "567a95c8ca676c1d07d5e3e7", "runid": "", "url_ResultView": "http://104.40.91.74:3200", "experiment_id": "895518857185768"}))

	PipelineNotification.PipelineNotification().completed_notification('5db2b88f7267f58cb8bf3998','567a95c8ca676c1d07d5e3e7','http://104.40.91.74:3200/pipeline/notify')
except Exception as ex: 
	PipelineNotification.PipelineNotification().failed_notification(ex,'5db2b88f7267f58cb8bf3998','567a95c8ca676c1d07d5e3e7','http://104.40.91.74:3200/pipeline/notify','http://104.40.91.74:3200/logs/getProductLogs')
	sys.exit(1)

