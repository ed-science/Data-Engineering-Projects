from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 load_sql="",
                 table_name="",
                 append_only=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.load_sql = load_sql
        self.table_name = table_name
        self.append_only = append_only

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info(f"Loading into {self.table_name} dimension table")
        self.log.info(f"Append only mode: {self.append_only}")
        if not self.append_only:
            sql_del_stmt = f'DELETE FROM {self.table_name}'
            redshift.run(sql_del_stmt)
        sql_stmt = f'INSERT INTO {self.table_name} {self.load_sql}'
        redshift.run(sql_stmt)
            
