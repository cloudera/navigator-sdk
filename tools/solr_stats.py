#! /usr/bin/env python
"""
Contains classes and functions to compute stats for Navigator Solr servers
and compare across deployments

Since this is focused on Navigator analysis as of 2.6.0,
it assumes Solr 4.10 and may break against Solr 5+
"""
from solr_client import SolrServer, SolrCore, frange
import pandas as pd
import re

USAGE = """
Usage: solr_stats.py <config_file_path> <output_xlsx_path>"

The config file should contain the details of all the Navigator deployments you
wish to compare. Each deployment's config should be one a separate line in the
format:

    name,host,port,user,password

e.g.,

    customer1,foo.cloudera.com,1234,user,password
    customer2,bar.cloudera.com,1234,user,password
"""

PATH_PATTEN = re.compile('(?:hdfs://[^/]*)(.*)')



class NavSolrServer(SolrServer):
    """
    Navigator Solr server is a SolrServer with convenience properties
    for 'nav_elements' and 'nav_relations', Navigator's 2 Solr cores.
    """

    @property
    def nav_elements(self):
        return self.get_core('nav_elements')

    @property
    def nav_relations(self):
        return NavRelations(self, 'nav_relations')

    def get_core(self, core):
        if core == 'nav_relations':
            return NavRelations(self, core)
        elif core == 'nav_elements':
            return NavCore(self, core)
        return super(NavSolrServer, self).get_core(core)



class NavCore(SolrCore):

    def get_docs(self, q='*:*', fq='', sort=None, wt='json',
                 rows=None, indent='true', batch_size=100000, params=None):
        if rows is None and sort is None:
            sort = 'identity asc'
        return super(NavCore, self).get_docs(q, fq, sort, wt, rows, indent,
                                             batch_size, params)

    def find_by_id(self, ids, fl=None):
        for part in partition(ids):
            for r in self.get_docs(fq=terms(part, self.schema.identity),
                                   params={'fl': fl}):
                yield r



def partition(lst, size=50*1024):
    for i in range(0, len(lst), size):
        yield lst[i:i+size]

def terms(ids, field):
    return '{{!terms f={}}}{}'.format(field, ','.join(ids))



class NavRelations(NavCore):

    def __init__(self, server, core):
        super(NavRelations, self).__init__(server, core)
        assert isinstance(server, NavSolrServer)

    def get_ep1_ids(self, relation_query):
        return self.get_endpoint_ids(relation_query, 'endpoint1Ids')

    def get_ep2_ids(self, relation_query, limit=None):
        return self.get_endpoint_ids(relation_query, 'endpoint2Ids')

    def get_endpoint_ids(self, relation_query, endpoint):
        ids = []
        for d in self.get_docs(relation_query,
                               params={'fl': ['identity', endpoint]}):
            ids.extend(d[endpoint])
        return ids



class NavSolrAnalyzer(object):
    """
    Class to get server level summary stats across cores, HDFS stats,
    and a breakdown of entity counts
    """

    def __init__(self, name, host, port, user, pwd, use_tls=False, verify=None):
        self.name = name
        self.server = NavSolrServer(host, port, user, pwd, use_tls=use_tls,
                                    verify=verify)
        self.nav_elements = self.server.nav_elements

    def summary_stats(self):
        """
        # docs and size for each core in the given SolrServer
        """
        resp = self.server.core_admin_status()
        core_names = []
        stats = ['indexHeapUsageBytes', 'numDocs', 'size']
        data = []
        for core, status in resp['status'].items():
            core_names.append(core)
            data.append([status['index'][field] for field in stats])
        return pd.DataFrame(data, index=pd.Index(core_names, name='core'),
                            columns=pd.Index(stats, name='stats')).T

    def hdfs_stats(self):
        """
        Get HDFS file size summary statistics (non-deleted only)
        """
        return self.nav_elements.stats(
            'size', fq='sourceType:HDFS AND type:FILE AND -deleted:true',
            stats=['max', 'sum', 'mean', 'stddev'])

    def count_breakdown(self):
        """
        Get entity counts by source type (HDFS, Yarn, etc) and entity type
        (File, Operation, etc) for non-deleted entities only
        """
        return self.nav_elements.pivot(fields=['sourceType', 'type'],
                                       fq='-deleted:true')

    def deleted_stats(self, fq='sourceType:HDFS'):
        fq += ('' if fq is None or fq == '' else ' AND ') + 'deleted:true'
        delete_time = self.nav_elements.stats(
            'deleteTime',
            fq=fq + 'AND {!frange l=0 incl=false}deleteTime',
            stats=['max', 'min'])
        min_date = int(delete_time.ix['min', 0])
        max_date = int(delete_time.ix['max', 0])
        day_in_millis = 1000 * 60 * 60 * 24
        break_points = [0, 1, 7, 30, 90, 365, 730]
        labels = ['1 day', '1 week', '1 month', '3 months', '1 year', '2 years',
                  'invalid deleteTime']
        queries = []
        for i in range(len(break_points)):
            u = max_date - break_points[i] * day_in_millis
            if i < len(break_points) - 1:
                l = max(min_date, max_date - break_points[i+1] * day_in_millis)
            else:
                l = min_date
            queries.append(self._make_deleteTime_query(l, u, label=labels[i]))
            if l == min_date:
                break
        queries.append(self._make_deleteTime_query(None, min_date, incl=True,
                                                   label='invalid deleteTime'))
        rs = self.nav_elements.facet_query(queries, fq=fq)
        rs = rs.reindex(labels).dropna().astype('int64')
        rs.name = 'Deleted'
        rs.index.name = 'Date Range'
        return rs.to_frame()

    def create_stats(self, start='NOW-1YEAR', end='NOW', gap='+1MONTH',
                     fq='sourceType:HDFS'):
        fq += (('' if fq is None or fq == '' else ' AND ') +
               'created:[* TO *] AND -deleted:true')
        ser = self.nav_elements.facet_range('created', start, end, gap, fq=fq)
        ser.index.name = 'Date'
        ser.name = 'Created'
        return ser.to_frame()

    def top_partitions(self, n=10):
        fq = 'type:PARTITION AND sourceType:HIVE AND -deleted:true'
        df = self.nav_elements.facet_field('parentPath', fq=fq,
                                           limit=n)
        df = df.rename(columns={'parentPath':'partition_count'})
        if n > 0:
            df = df[:n]
        self._add_hdfs_subdir_counts(df)
        return df

    def _add_hdfs_subdir_counts(self, df):
        dbs = []
        tables = []
        for parent_path in df.index:
            db, table = tuple([x for x in parent_path.split('/')
                               if len(x) > 0])
            dbs.append(db)
            tables.append(table)
        df['Database'] = dbs
        df['Table'] = tables
        df['hdfs_subdir_count'] = self._get_hdfs_subdir_count(dbs, tables)
        return df.reset_index()

    def _get_hdfs_subdir_count(self, db_names, table_names):
        counts = []
        for i in range(0, len(db_names), 30):
            counts.extend(self._subdir_helper(db_names[i:i+30],
                                              table_names[i:i+30]))
        return counts

    def _subdir_helper(self, db_names, table_names):
        clause_base = '(parentPath:\/{} AND originalName:{})'
        lst = []
        for db_name, table_name in zip(db_names, table_names):
            lst.append(clause_base.format(db_name, table_name))
        table_query = ('sourceType:HIVE AND type:TABLE AND (' +
                       ' OR '.join(lst) + ')')
        tables = self.nav_elements.get_docs(
            fq=table_query, rows=len(db_names))
        count_map = {} # (db, tbl) -> count
        for t in tables:
            key = (t['parentPath'][1:], t['originalName'])
            path = _get_file_system_path(t['fileSystemPath'])
            query = ('sourceType:HDFS AND type:DIRECTORY AND '
                     '-fileSystemPath:\{0}/*.hive-staging* AND '
                     'fileSystemPath:\{0}/*').format(path)
            count_map[key] = self.nav_elements.get_count(query)
        counts = []
        for db_name, table_name in zip(db_names, table_names):
            counts.append(count_map[(db_name, table_name)])
        return counts

    def _make_deleteTime_query(self, l, u, incl=False, incu=True,
                               label=None):
        return frange('deleteTime', l, u, incl, incu, label)


def _get_file_system_path(full_path):
    """
    hdfs://Enchilada/path -> path
    """
    matches = PATH_PATTEN.match(full_path)
    return matches.group(1)


class NavSolrComparator(object):

    def __init__(self, analyzers):
        self.analyzers = analyzers

    def summary_stats(self):
        return self._compare('summary_stats')

    def hdfs_stats(self):
        return self._compare('hdfs_stats').reorder_levels([1,0], 1)

    def count_breakdown(self):
        return self._compare('count_breakdown').reorder_levels([1,0], 1)

    def deleted_stats(self, fq=None):
        return self._compare('deleted_stats', fq=fq)

    def create_stats(self, fq=None):
        return self._compare('create_stats', fq=fq)

    def _compare(self, meth, *args, **kwds):
        args = list(zip(*[(getattr(a, meth)(*args, **kwds), a.name)
                          for a in self.analyzers]))
        return self._concat(args[0], args[1])

    def _concat(self, lst, names):
        return pd.concat(lst, axis=1, keys=names)



def to_excel(comparator, path):
    writer = pd.ExcelWriter(path)
    comparator.summary_stats().to_excel(writer, 'Summary Stats')
    comparator.hdfs_stats().to_excel(writer, 'HDFS Stats')
    comparator.count_breakdown().to_excel(writer, 'Counts Breakdown')
    comparator.deleted_stats(fq='sourceType:HDFS').to_excel(
        writer, 'Deleted HDFS Entities')
    comparator.create_stats(fq='sourceType:HDFS').to_excel(
        writer, 'Created HDFS Entities')
    comparator.create_stats(fq='sourceType:HIVE AND type:TABLE').to_excel(
        writer, 'Created Hive Tables')
    writer.save()
    writer.close()



if __name__=='__main__':
    import sys
    if len(sys.argv) < 3:
        sys.exit(USAGE)
    conf_file = sys.argv[1]
    with open(conf_file, 'r') as fh:
        configs = [l.split(',') for l in fh.readlines()
                   if not l.startswith('#')]
    path = sys.argv[2]

    if len(configs) > 1:
        comp = NavSolrComparator([NavSolrAnalyzer(*conf) for conf in configs])
    else:
        comp = NavSolrAnalyzer(*configs[0])
    to_excel(comp, path)
