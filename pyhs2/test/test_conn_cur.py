import unittest, tempfile, pyhs2, os, stat
from pyhs2.connections import Connection

class TestConnCur(unittest.TestCase):

    def setUp(self):
        self.hive_cfg = {'host' : "localhost", 'port' : 10000,
                         'authMechanism' : "PLAIN", 'user' : "bruderman",
                         'password' : "brad"}
        self.test_db  = 'test_db_4_pyhs2'
        self.test_tbl = 'pyhs2_test_tbl'
        testdata_file = tempfile.NamedTemporaryFile(mode='wb', delete=False)
        setup_hqls    = ['CREATE DATABASE IF NOT EXISTS {}'.format(self.test_db),
                         'USE {}'.format(self.test_db),
                         "CREATE TABLE IF NOT EXISTS {} (STR_COL STRING, BIGINT_COL BIGINT) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'".format(self.test_tbl),
                         "LOAD DATA LOCAL INPATH '{fp}' INTO TABLE {tbl}".format(fp=testdata_file.name, tbl=self.test_tbl)]
        self.test_data = [['str_%d' % i, i] for i in range(1000000,1020010)]
        testdata_file.write('\n'.join( ( '\t'.join((str(s) for s in td)) for td in self.test_data)))
        testdata_file.flush()
        os.chmod(testdata_file.name, stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)

        with Connection(**self.hive_cfg) as conn:
            with conn.cursor() as cur:
                map(cur.execute, setup_hqls)
            
        testdata_file.close()

    def test_result_sets_match(self):
        for lim in (0,100,10000,11000,20000,21000):
            with pyhs2.connect(**self.hive_cfg) as conn:
                with conn.cursor() as cur:
                    cur.execute('SELECT * FROM {db}.{tbl} LIMIT {limit}'.format(db=self.test_db, tbl=self.test_tbl, limit=lim))
                    res = cur.fetch()
            self.assertEqual(res, self.test_data[:lim])

    def tearDown(self):
        with Connection(**self.hive_cfg) as conn:
            with conn.cursor() as cur:
                cur.execute('DROP DATABASE IF EXISTS {} CASCADE'.format(self.test_db))
              
if __name__ == '__main__':
    suite = unittest.TestLoader().loadTestsFromTestCase(TestConnCur)
    unittest.TextTestRunner(verbosity=2).run(suite)
