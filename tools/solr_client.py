"""
A light-weight python client for Solr servers tested on 4.10
"""
import pandas as pd
import requests

class SolrServer(object):
    """
    Encapsulates connection to a given Solr server
    """
    def __init__(self, host, port, user, pwd, debug=False, use_tls=False,
                 verify=None):
        """
        Parameters
        ----------
        host: str
          e.g., vc0330.halxg.cloudera.com
        port: int
          e.g., 7512
        user: str
          user name
        pwd: str
          password
        """
        self.host = host
        self.port = int(port)
        self.user = user
        self.pwd = pwd
        self.use_tls = use_tls
        self.verify = verify
        self._debug = debug
        self._session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(max_retries=10)
        self._session.mount('http://', adapter)

    @property
    def url(self):
        if self.use_tls:
            protocol = 'https'
        else:
            protocol = 'http'
        return '{}://{}:{}/solr'.format(protocol, self.host, self.port)

    def post(self, url, data=None):
        if self._debug:
            print('Query: ' + str(data))
        return self._session.post(url, auth=(self.user, self.pwd), data=data,
                                  verify=self.verify)

    def get(self, url, params=None):
        if self._debug:
            print('Query: ' + str(params))
        return self._session.get(url, auth=(self.user, self.pwd),
                                 params=params, verify=self.verify)

    def get_core(self, core_name):
        return SolrCore(self, core_name)

    def core_admin_status(self, name=None):
        """
        Check core admin status and return results in json.
        If name is unspecified, returns all cores.
        """
        params={'action':'STATUS', 'wt':'json'}
        if name is not None:
            params['core'] = name
        result = self.get(self.url + '/admin/cores', params=params)
        if result.status_code == requests.codes.forbidden:
            raise Exception('Access to Solr cores is forbidden. ' + \
                            'Please ensure Dev Mode is enabled in Navigator.')
        return result.json()



class SolrCore(object):
    """
    Client to execute queries against a core on a given Solr server.
    """

    def __init__(self, server, core):
        """
        Parameters
        ----------
        server: SolrServer
        core: str
          name of the core 'nav_elements' or 'nav_relations'
        """
        self.server = server
        self.core = core
        self._schema = None

    @property
    def url(self):
        """
        Nav does not have TLS auth as of 2.6.0 so always http
        """
        return '{}/{}'.format(self.server.url, self.core)

    @property
    def schema(self):
        if self._schema is None:
            self._schema = SolrSchema(self)
        return self._schema

    def query(self, q='*:*', fq='', sort='', wt='json', rows=0, indent='true',
              params=None):
        """
        We're not using ** magic here so we can do parameters like 'facet.pivot'
        """
        q_params = {
            'q': q,
            'fq': fq,
            'rows': rows,
            'indent': indent,
            'wt': wt,
            'sort': sort
        }
        if params is not None:
            q_params.update(params)
        return self.server.post('{}/select'.format(self.url),
                               data=q_params).json()

    def get_docs(self, q='*:*', fq='', sort='', wt='json', rows=None,
                 indent='true', batch_size=100000, params=None):
        """
        Generator
        """
        if params is None:
            params = {}
        _docs = lambda resp: resp.get('response').get('docs')
        if rows is not None:
            for d in _docs(self.query(q=q, fq=fq, sort=sort, wt=wt, rows=rows,
                                      indent='true', params=params)):
                yield d
        else:
            assert sort is not None and len(sort) > 0
            cursor = '*'
            while True:
                params['cursorMark'] = cursor
                batch = self.query(q=q, fq=fq, sort=sort, wt=wt, rows=batch_size,
                                   indent='true', params=params)
                for d in _docs(batch):
                    yield d
                cursor = batch['nextCursorMark']
                if params['cursorMark'] == cursor:
                    break

    def get_count(self, q='*:*', fq='', params=None):
        if params is None:
            params = {}
        resp = self.query(q=q, fq=fq, rows=0, params=params)
        return resp['response']['numFound']

    def pivot(self, fields, fq=''):
        """
        Execute a facet pivot query and return results as a DataFrame

        e.g, solr_core.pivot(['sourceType', 'type']) returns a DataFrame like:

                                        count
        sourceType	type
        hdfs		                    22660220
                    file	            17846879
                    directory	        3880049
                    source	            1
        hive		                    5395137
                    field	            2553551
                    sub_operation	    1827283
                    partition	        599414
                    operation_execution	258680
                    operation	        97738
                    table	            52900
                    view	            3310
                    database	        2260
                    source              1
        ... ...

        Parameters
        ----------
        fields: list-like
          facet.pivot field names
        fq: str, default ''
          Filter query
        """
        json_resp = self.query(fq=fq, params={'facet':'true',
                                              'facet.pivot':','.join(fields)})
        pivot_data = json_resp['facet_counts']['facet_pivot'][','.join(fields)]
        levels = len(fields)
        cols = list(fields)
        cols.append('count')
        flattened = _flatten(pivot_data, [], levels)
        return pd.DataFrame(flattened, columns=cols).set_index(fields)

    def stats(self, fields, fq='', stats=None):
        """
        Execute a Solr stats query and return results as a DataFrame

        e.g., solr_core.stats('size', stats=['max', 'sum', 'mean', 'stddev']
        Returns a DataFrame like:

                size
        max	    3.64468e+12
        sum	    3.96866e+14
        mean	2.22373e+07
        stddev	2.00506e+09

        Parameters
        ----------
        fields: str or list
          Fields to get stats on
        fq: str, default ''
          Filter query
        stats: list, optional
          List of stats names, or all stats if unspecified
        """
        json_resp = self.query(fq=fq, params={'stats':'true',
                                              'stats.field':fields})
        stats_data = json_resp['stats']['stats_fields']
        df = pd.DataFrame.from_dict(stats_data)
        if stats is not None:
            df = df.reindex(stats)
        df.index.name = 'stats'
        return df

    def facet_field(self, fields, fq='', limit=100, mincount=1):
        json_resp = self.query(fq=fq, params={'facet':'true',
                                              'facet.field':fields,
                                              'facet.limit':limit,
                                              'facet.mincount':mincount})
        raw_data = json_resp['facet_counts']['facet_fields']
        reshaped = {}
        for field_name, field_data in raw_data.iteritems():
            labels = []
            values = []
            for i in range(int(len(field_data) / 2)):
                labels.append(field_data[i * 2])
                values.append(field_data[i * 2 +1])
            labels = pd.Index(labels, name=field_name)
            reshaped[field_name] = pd.Series(values, labels, name='Count')
        return pd.DataFrame(reshaped)

    def facet_query(self, queries, fq=''):
        json_resp = self.query(fq=fq, params={'facet':'true',
                                              'facet.query':queries})
        data = json_resp['facet_counts']['facet_queries']
        return pd.Series(data)

    def facet_range(self, field, start, end, gap, fq=''):
        json_resp = self.query(fq=fq, params={'facet': 'true',
                                              'facet.range': field,
                                              'facet.range.start': start,
                                              'facet.range.end': end,
                                              'facet.range.gap': gap})
        data = json_resp['facet_counts']['facet_ranges'][field]['counts']
        labels = []
        values = []
        for i in range(int(len(data) / 2)):
            labels.append(data[i * 2])
            values.append(data[i * 2 +1])
        return pd.Series(values, pd.to_datetime(labels))



class SolrSchema(object):

    def __init__(self, core):
        self._core = core
        schema = core.server.get('{}/schema'.format(core.url)).json()
        for f in schema['schema']['fields']:
            field = SolrField(self, f['name'], f['type'], f['multiValued'],
                              f['indexed'], f['stored'])
            setattr(self, field.name, field)



class SolrField(object):

    def __init__(self, schema, name, field_type, multi, indexed, stored):
        self.schema = schema
        self.name = name
        self.field_type = field_type
        self.multi = multi
        self.indexed = indexed
        self.stored = stored

    def __repr__(self):
        return self.name



def frange(field, l, u, incl=True, incu=False, key=None):
    incl = str(incl).lower()
    incu = str(incu).lower()
    key = '' if key is None else ' key="' + key + '"'
    query = '{!frange'
    if l is not None:
        query += ' l={l} incl={incl}'.format(**locals())
    if u is not None:
        query += ' u={u} incu={incu}'.format(**locals())
    query += key + '}' + field
    return query

def _flatten(lst, prefixes, tot_levels):
    """
    Convert the response from facet pivot API into a flattened list
    """
    rs = []
    for rec in lst:
        row = list(prefixes)
        row.append(rec['value'])
        diff = tot_levels - len(row)
        if diff > 0:
            row.extend([' '] * diff) # use space so Excel doesn't merge cells
        row.append(rec['count'])
        rs.append(row)
        if 'pivot' in rec:
            next_pref = list(prefixes)
            next_pref.append(rec['value'])
            rs.extend(_flatten(rec['pivot'], next_pref, tot_levels))
    return rs
