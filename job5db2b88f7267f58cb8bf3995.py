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

try: 
	PipelineNotification.PipelineNotification().started_notification('5db2b88f7267f58cb8bf3996','567a95c8ca676c1d07d5e3e7','http://104.40.91.74:3200/pipeline/notify')
	Movie_Recc_dbfs = DBFSConnector.DBFSConnector.fetch([], {}, "5db2b88f7267f58cb8bf3996", spark, "{'url': '/Demo/Marketing/MovieRatings (2).csv', 'file_type': 'Delimeted', 'dbfs_token': 'dapi743e2d3cc92a32916f8c2fa9bd7d0606', 'dbfs_domain': 'westus.azuredatabricks.net', 'delimiter': ',', 'is_header': 'Use Header Line'}")

	PipelineNotification.PipelineNotification().completed_notification('5db2b88f7267f58cb8bf3996','567a95c8ca676c1d07d5e3e7','http://104.40.91.74:3200/pipeline/notify')
except Exception as ex: 
	PipelineNotification.PipelineNotification().failed_notification(ex,'5db2b88f7267f58cb8bf3996','567a95c8ca676c1d07d5e3e7','http://104.40.91.74:3200/pipeline/notify','http://104.40.91.74:3200/logs/getProductLogs')
try: 
	PipelineNotification.PipelineNotification().started_notification('5db2b88f7267f58cb8bf3997','567a95c8ca676c1d07d5e3e7','http://104.40.91.74:3200/pipeline/notify')
	Movie_Recc_FE = TranformationsMainFlow.TramformationMain.run(["5db2b88f7267f58cb8bf3996"],{"5db2b88f7267f58cb8bf3996": Movie_Recc_dbfs}, "5db2b88f7267f58cb8bf3997", spark,json.dumps( {"FE": [{"transformationsData": {}, "feature": "UserId", "transformation": "", "type": "numeric", "selected": "True", "stats": {"count": "1081", "mean": "461.71", "stddev": "261.55", "min": "1", "max": "943", "missing": "0"}}, {"transformationsData": {}, "feature": "MovieId", "transformation": "", "type": "numeric", "selected": "True", "stats": {"count": "1081", "mean": "438.13", "stddev": "337.38", "min": "1", "max": "1648", "missing": "0"}}, {"transformationsData": {}, "feature": "Rating", "transformation": "", "type": "real", "selected": "True", "stats": {"count": "1081", "mean": "3.5", "stddev": "1.18", "min": "1.0", "max": "5.0", "missing": "0"}}, {"transformationsData": {"feature_label": "Timestamp"}, "feature": "Timestamp", "transformation": "Extract Date", "type": "date", "selected": "True", "stats": {"count": "", "mean": "", "stddev": "", "min": "", "max": "", "missing": "0"}}, {"transformationsData": {}, "feature": "AvgRating", "transformation": "", "type": "real", "selected": "True", "stats": {"count": "1081", "mean": "3.54", "stddev": "0.44", "min": "1.51", "max": "4.59", "missing": "0"}}, {"feature": "Timestamp_dayofmonth", "transformation": "", "transformationsData": {}, "type": "numeric", "selected": "True", "stats": {"count": "1081", "mean": "16.48", "stddev": "9.14", "min": "1", "max": "31", "missing": "0"}}, {"feature": "Timestamp_month", "transformation": "", "transformationsData": {}, "type": "numeric", "selected": "True", "stats": {"count": "1081", "mean": "7.1", "stddev": "4.32", "min": "1", "max": "12", "missing": "0"}}, {"feature": "Timestamp_year", "transformation": "", "transformationsData": {}, "type": "numeric", "selected": "True", "stats": {"count": "1081", "mean": "1997.44", "stddev": "0.5", "min": "1997", "max": "1998", "missing": "0"}}]}))

	PipelineNotification.PipelineNotification().completed_notification('5db2b88f7267f58cb8bf3997','567a95c8ca676c1d07d5e3e7','http://104.40.91.74:3200/pipeline/notify')
except Exception as ex: 
	PipelineNotification.PipelineNotification().failed_notification(ex,'5db2b88f7267f58cb8bf3997','567a95c8ca676c1d07d5e3e7','http://104.40.91.74:3200/pipeline/notify','http://104.40.91.74:3200/logs/getProductLogs')
try: 
	PipelineNotification.PipelineNotification().started_notification('5db2b88f7267f58cb8bf3998','567a95c8ca676c1d07d5e3e7','http://104.40.91.74:3200/pipeline/notify')
	Movie_Recommendation_AutoML = tpot_execution.Tpot_execution.run(["5db2b88f7267f58cb8bf3997"],{"5db2b88f7267f58cb8bf3997": Movie_Recc_FE}, "5db2b88f7267f58cb8bf3998", spark,json.dumps( {"model_type": "regression", "label": "AvgRating", "run_id": "0c2862c909764fdb9523ae7a42351516", "features": ["UserId", "MovieId", "Rating", "Timestamp", "Timestamp_dayofmonth", "Timestamp_month", "Timestamp_year"], "percentage": "30", "executionTime": 6, "sampling": "0", "sampling_value": "", "model_id": "5e04bde51bfdaec91f2f9ca3", "ProjectName": "Retail Scenarios", "PipelineName": "MoviePredictionSystem", "userid": "567a95c8ca676c1d07d5e3e7", "url_ResultView": "http://104.40.91.74:3200", "experiment_id": "895518857185768"}))

	PipelineNotification.PipelineNotification().completed_notification('5db2b88f7267f58cb8bf3998','567a95c8ca676c1d07d5e3e7','http://104.40.91.74:3200/pipeline/notify')
except Exception as ex: 
	PipelineNotification.PipelineNotification().failed_notification(ex,'5db2b88f7267f58cb8bf3998','567a95c8ca676c1d07d5e3e7','http://104.40.91.74:3200/pipeline/notify','http://104.40.91.74:3200/logs/getProductLogs')

