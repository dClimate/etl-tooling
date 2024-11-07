import abc

class Assessor(abc.ABC):
    """A component responsible for assessing if the pipeline should run."""

    @abc.abstractmethod
    def start(self, **kwargs):
        """Start the analysis."""
