import os
import unittest
from astropy.io import fits

from csst_dfs_api_cluster.facility.level0 import Level0DataApi

class Level0DataTestCase(unittest.TestCase):

    def setUp(self):
        self.api = Level0DataApi()

    def test_find(self):
        recs = self.api.find(obs_id = 9, obs_type = 'sci', limit = 0)
        print('find:', recs)

    def test_get(self):
        rec = self.api.get(fits_id = 100)
        print('get:', rec)

    def test_update_proc_status(self):
        rec = self.api.update_proc_status(fits_id = 100, status = 6)
        print('update_proc_status:', rec)

    def test_update_qc0_status(self):
        rec = self.api.update_qc0_status(fits_id = 100, status = 7)
        print('update_qc0_status:', rec)