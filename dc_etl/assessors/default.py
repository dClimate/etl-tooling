from dc_etl.assessor import Assessor

class DefaultAssessor(Assessor):
    """Default Assessor.

    """
    def start(self, **kwargs):
        """Start the analysis."""
        return True