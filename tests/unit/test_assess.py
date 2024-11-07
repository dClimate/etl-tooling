
from dc_etl import component



class TestDefaultAssessor:
    def test_assess(self, cache):
        assessor = component.assessor("default")
        assessor.start()
        assert True