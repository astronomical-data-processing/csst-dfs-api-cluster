import unittest

from csst_dfs_api_cluster.facility.detector import DetectorApi

class DetectorApiTestCase(unittest.TestCase):

    def setUp(self):
        self.api = DetectorApi()

    def test_find(self):
        recs = self.api.find(module_id = 'MSC', key = 'CCD')
        print('find:', recs)

    def test_get(self):
        rec = self.api.get(no = '02')
        print('get:', rec)

    def test_write(self):
        rec = self.api.write(no = '05', 
                detector_name = 'CCD05', 
                module_id = 'MSC', 
                filter_id='f5')
        print('write:', rec)

    def test_update(self):
        rec = self.api.update(no = '01', filter_id = 'f1')
        print('update:', rec)

    def test_delete(self):
        rec = self.api.delete(no = '01')
        print('delete:', rec)

    def test_find_status(self):
        recs = self.api.find_status(detector_no = '01', 
            status_occur_time = ('2021-06-02','2021-06-08'), 
            limit = 0)
        print('find status:', recs)

    def test_get_status(self):
        rec = self.api.get_status(id = 2)
        print('get status:', rec)

    def test_write_status(self):
        rec = self.api.write_status(detector_no = '01', status = '{........}',status_time='2021-06-05 12:12:13')
        print('write status:', rec)