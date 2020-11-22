from datetime import timedelta
from cogflow.hooks import TimeSeriesHook
from airflow.utils.decorators import apply_defaults
from airflow.models.baseoperator import BaseOperator


class TimeSeriesOperator(BaseOperator):

    template_fields = ('_start_date', '_end_date', '_output_path')

    @apply_defaults
    def __init__(
        self,
        conn_id,
        output_path,
        start_date,
        end_date,
        date_offset,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.conn_id = conn_id
        self._output_path = output_path
        self._start_date = start_date - timedelta(days=date_offset)
        self._end_date = end_date - timedelta(days=date_offset)

    def execute(self, context):
        hook = TimeSeriesHook(conn_id=self._conn_id)

        try:
            self.log.info(
                f'Fetching time series from {self._start_date}'
                f'to {self._end_date}.'
            )

            time_series = get_time_series(
                ids=self._ids,
                start_date=self._start_date,
                end_date=self._end_date,
                **kwargs
            )
            self.log.info(f'Fetched {len(time_series)} records.')
        finally:
            hook.close()

         # Make sure the output directory exists.
        output_dir = os.path.dirname(self._output_path)
        os.makedirs(output_dir, exist_ok=True)

        with open(self._output_path, "w") as handle:
            json.dump(ratings, fp=handle)
