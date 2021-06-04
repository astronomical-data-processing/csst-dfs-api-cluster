import os
import unittest
from astropy.io import fits

from csst_dfs_api_cluster.common.catalog import CatalogApi

class CommonEphemTestCase(unittest.TestCase):

    def setUp(self):
        self.api = CatalogApi()

    def test_gaia3_query(self):
        recs = self.api.gaia3_query(ra=222.1, dec=40, radius=0.5, min_mag=-1, max_mag=-1, obstime = -1, limit = 2)
        print('find:', recs)
