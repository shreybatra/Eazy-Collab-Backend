from django.conf import settings
from elasticsearch import Elasticsearch

import ujson
from commons.utils.loggers import app_logger


class ESQuery:
    '''Base Class for generating, executing elasticsearch query'''

    def __init__(self, url_parts, logger=app_logger, q_attribs=None):

        self.__q = {}
        self.__url_parts = {}
        self.es_settings = settings.DATABASE_SETTINGS.get('elasticsearch', {})

        if not self.es_settings:
            raise ValueError('Could not find elasticsearch in DATABASE_SETTINGS')

        url_defaults = {
            'proto': 'http',
            'hosts': self.es_settings.get('HOSTS', []),
            'index': None,
            'doc_type': None,
            'method': '_search',
            'is_scroll': False,
            'scroll': '3m',
            'export': False,
            '_id': None,
            'body': None,
        }

        for url_part, part_default in url_defaults.items():
            self.__url_parts[url_part] = url_parts.get(url_part, part_default)

        if q_attribs:
            self.set_q_attribs(q_attribs)

        self.logger = logger

    def set_q_attribs(self, q_attribs):
        if q_attribs.get('is_filtered'):
            self.__q = {
                'query': {
                    'filtered': {
                        'query': {
                            'match_all': {}
                        },
                        'filter': {}
                    }
                }
            }

            if q_attribs.get('filters'):
                self.__q['query']['filtered']['filter'] = q_attribs.get(
                    'filters')

            if q_attribs.get('base_query'):
                self.__q['query']['filtered']['query'] = q_attribs.get(
                    'base_query')
        else:
            self.__q = {
                'query': {
                    'match_all': []
                }
            }
            if q_attribs.get('base_query'):
                self.__q['query'] = q_attribs.get('base_query')

        if q_attribs.get('aggs'):
            self.__q['aggs'] = q_attribs.get('aggs')

    def set_filters(self, filters):
        self.__q['query']['filtered']['filter'] = filters

    def get_filters(self):
        return self.__q['query']['filtered']['filter']

    def set_sort(self, sort_filter=None):
        if sort_filter:
            if 'sort' not in self.__q:
                self.__q['sort'] = []

            if isinstance(sort_filter, list):
                self.__q['sort'].extend(sort_filter)
            else:
                self.__q['sort'].append(sort_filter)

    def get_sort(self):
        return self.__q.get('sort', [])

    def set_query_search(self, search_filter):
        if '*' != search_filter[-1]:
            search_filter += '*'
        self.__q['query']['filtered']['query'] = {
            'query_string': {'query': search_filter}}

    def get_query_search(self):
        return self.__q['query']['filtered']['query']['query_string']['query']

    def set_aggs(self, aggs):
        self.__q['aggs'] = aggs

    def get_aggs(self):
        if 'aggs' not in self.__q:
            self.__q['aggs'] = {}
        return self.__q['aggs']

    def set_fields(self, fields):
        self.__q['fields'] = fields

    def unset_fields(self):
        if 'fields' in self.__q:
            del self.__q['fields']

    def get_fields(self):
        return self.__q['fields']

    def set_source(self, fields):
        self.__q['_source'] = fields

    def get_source(self):
        return self.__q['_source']

    def set_includes(self, fields):
        if '_source' not in self.__q:
            self.__q['_source'] = {}
        self.__q['_source']['includes'] = fields

    def get_includes(self):
        return self.__q['_source']['includes']

    def set_excludes(self, fields):
        if '_source' not in self.__q:
            self.__q['_source'] = {}
        self.__q['_source']['excludes'] = fields

    def get_excludes(self):
        return self.__q['_source']['excludes']

    def set_size(self, size):
        self.__q['size'] = size

    def get_size(self):
        return self.__q['size']

    def set_offset(self, offset):
        self.__q['from'] = offset

    def get_offset(self):
        return self.__q['from']

    def set_script(self, script):
        self.__q['script_fields'] = script

    def get_script(self, script):
        return self.__q['script_fields']

    def set_scroll_id(self, scroll, scroll_id):
        self.__q = {'scroll': scroll, 'scroll_id': scroll_id}

    def set_scroll(self, is_scroll=None, scroll=None):
        if is_scroll:
            self.__url_parts['is_scroll'] = is_scroll
        if scroll:
            self.__url_parts['scroll'] = scroll

    def set_scroll_export(self, export):
        if export:
            self.__url_parts['is_scroll'] = True
            self.__url_parts['export'] = export

    def unset_es_url_params(self):
        self.__url_parts.pop('index', None)
        self.__url_parts.pop('doc_type', None)

    def set_query(self, q):
        self.__q = q

    def get_query(self):
        return self.__q

    def execute(self, method='GET', log=True):

        serial_q = ujson.dumps(self.__q)
        index = ''

        if self.__url_parts.get('index'):

            index = self.__url_parts['index']
            if not index.startswith(self.es_settings['ENVIRONMENT'], 0):
                self.__url_parts['index'] = self.es_settings['ENVIRONMENT'] + '_' + self.__url_parts['index']
            index = self.__url_parts['index']

        if log:
            self.log_query(
                str(self.__url_parts['hosts']) + ' : ' + index + ' : ' + (self.__url_parts.get('doc_type', '') or '') +
                ' : ' + self.__url_parts['method'], serial_q
            )
        if not self.__url_parts['is_scroll']:
            key = (
                {
                    'index': self.__url_parts['index'],
                    'doc_type': self.__url_parts['doc_type'],
                    'q': serial_q,
                    'method': method
                }
            )

        try:
            r = {}
            es = Elasticsearch(hosts=self.__url_parts['hosts'], timeout=45)

            if not es.ping():
                raise ValueError('Connection failed')

            # In case of scroll api, not checking if index is present or not, as scroll api request do not have index
            # & doc_type. during each scroll batch request, self.__url_parts.get('is_scroll') is set to true.

            if not self.__url_parts.get('is_scroll') and not es.indices.exists(index=self.__url_parts['index']):
                return {}

            if method.upper() == 'GET':
                if self.__url_parts['method'] == '_count':
                    r = es.count(
                        index=self.__url_parts['index'], doc_type=self.__url_parts['doc_type'], body=serial_q)

                elif self.__url_parts['method'] == '_mapping':
                    r = es.indices.get_mapping(
                        self.__url_parts['index'], self.__url_parts['doc_type'])

                else:
                    r = es.get(
                        self.__url_parts['index'], self.__url_parts['_id'], doc_type=self.__url_parts['doc_type'])

            elif method.upper() == 'POST':

                if self.__url_parts['method'] == '_search':

                    if self.__q.get('scroll_id'):
                        r = es.scroll(
                            scroll_id=self.__q['scroll_id'], scroll=self.__q['scroll'])

                    elif self.__url_parts.get('is_scroll'):
                        r = es.search(
                            index=self.__url_parts['index'],
                            doc_type=self.__url_parts['doc_type'],
                            body=serial_q,
                            scroll=self.__url_parts['scroll']
                        )
                    else:
                        r = es.search(
                            index=self.__url_parts['index'], doc_type=self.__url_parts['doc_type'], body=serial_q)

                elif self.__url_parts['method'] == '_count':
                    r = es.count(
                        index=self.__url_parts['index'], doc_type=self.__url_parts['doc_type'], body=serial_q)

                elif self.__url_parts['method'] == '_bulk':
                    r = es.bulk(self.__url_parts['body'], index=self.__url_parts['index'],
                                doc_type=self.__url_parts['doc_type'], params=None)

                elif self.__url_parts['method'] == '_update':
                    r = es.update(
                        index=self.__url_parts['index'], doc_type=self.__url_parts['doc_type'], body=serial_q)

                elif self.__url_parts['method'] == '_delete_by_query':
                    r = es.delete_by_query(
                        self.__url_parts['index'], serial_q, doc_type=self.__url_parts['doc_type'])

                elif self.__url_parts['method'] == '_update_by_query':
                    r = es.update_by_query(
                        index=self.__url_parts['index'], doc_type=self.__url_parts['doc_type'], body=serial_q)

                elif self.__url_parts['method'] == '_insert':
                    r = es.index(
                        index=self.__url_parts['index'],
                        doc_type=self.__url_parts['doc_type'],
                        body=serial_q,
                        id=self.__url_parts['_id']
                    )

            elif method.upper() == 'PUT':
                if self.__url_parts['method'] == '_mapping':
                    r = es.indices.put_mapping(
                        self.__url_parts['doc_type'], serial_q, index=self.__url_parts['index'])
                else:
                    r = es.create(index=self.__url_parts['index'], doc_type=self.__url_parts['doc_type'],
                                  id=self.__url_parts['_id'], body=self.__url_parts['body'])
            elif method.upper() == 'DELETE':
                r = es.delete(
                    index=self.__url_parts['index'], doc_type=self.__url_parts['doc_type'], id=self.__url_parts['_id'])

            return r
        except Exception as e:
            self.logger.error(str(e))
            raise

    def log_query(self, url, serial_q):
        self.logger.info(url)
        self.logger.info(serial_q)

    @staticmethod
    def get_data(data, is_count=False):
        if is_count:
            return data['count']
        else:
            rows = []
            hits = data['hits']['hits']
            for hit in hits:
                rows.append(hit['_source'])
            return rows

    @staticmethod
    def get_distinct_values(data, group_field):
        distinct_values = map(lambda x: x.get(
            'key_as_string') or x['key'], data['aggregations'][group_field]['buckets'])
        return distinct_values
