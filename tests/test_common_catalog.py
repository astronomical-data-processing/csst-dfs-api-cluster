import os
import unittest
from astropy.io import fits

from csst_dfs_api_cluster.common.catalog import CatalogApi

class CommonEphemTestCase(unittest.TestCase):

    def setUp(self):
        self.api = CatalogApi()

    def test_gaia3_query(self):
        recs = self.api.gaia3_query(ra=56.234039029108935, dec=14.466534827703473, radius=4, min_mag=-1, max_mag=-1, obstime = -1, limit = 2)
        print('find:', recs)
