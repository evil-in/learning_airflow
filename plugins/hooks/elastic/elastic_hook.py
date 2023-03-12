from airflow.plugins_manager import AirflowPlugin # To create a plugin
from airflow.hooks.base import BaseHook # Gives properities to all hooks in Airflow

from elasticsearch import Elasticsearch

class ElasticHook(BaseHook):

    def __init__(self, conn_id='elastic_default', *args,**kwargs):
        super().__init__(*args, **kwargs) #Initializing the base hook class
        conn = self.get_connection(conn_id)

        conn_config = {}
        hosts = []

        if conn.host:
            hosts = conn.host.split(',')
        if conn.port:
            conn_config['port'] = int(conn.port)
        if conn.login:
            conn_config['http_auth'] = (conn.login, conn.password)

        self.es = Elasticsearch(hosts, **conn_config)
        self.index = conn.schema

    def info(self):
        return self.es.info()

    def set_index(self, index):
        self.index = index

    def add_doc(self, index, doc_type, doc): # fn to add a doc to ES
        self.set_index(index)
        res = self.es.index(index=index, doc_type=doc_type, doc= doc)
        return res
    

class AirflowElasticPlugin(AirflowPlugin):
    name = 'elastic'
    hooks = [ElasticHook]






 

