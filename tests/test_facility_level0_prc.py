import os
import unittest
from astropy.io import fits

from csst_dfs_api_cluster.facility.level0prc import Level0PrcApi

class Level0PrcTestCase(unittest.TestCase):

    def setUp(self):
        self.api = Level0PrcApi()

    def test_find(self):
        recs = self.api.find(level0_id=134)
        print('find:', recs)

    def test_update_proc_status(self):
        rec = self.api.update_proc_status(id = 8, status = 4)
        print('update_proc_status:', rec)

    def test_write(self):
        rec = self.api.write(level0_id=134, 
            pipeline_id = "P1",
            prc_module = "QC0",
            params_id = "/opt/dddasd.params",
            prc_status = 3,
            prc_time = '2021-06-04 11:12:13',
            file_path = "/opt/dddasd.header")
        print('write:', rec)