import fsspec

from multiformats import CID

from dc_etl.filespec import FileSpec
from dc_etl.ipld.local_file import LocalFileIPLDPublisher


class TestLocalFileIPLDPublisher:
    @staticmethod
    def test_publish_retrieve(tmpdir):
        cid = CID.decode("bafyreic5rlxomntm5as6dwi3nwsfueq7vqcfqoqwqu5y3xuu6w5nyichpq")
        path = FileSpec(fsspec.filesystem("file"), str(tmpdir)) / "test.cid"
        publisher = LocalFileIPLDPublisher(path)
        assert publisher.retrieve() is None
        publisher.publish(cid)
        assert publisher.retrieve() == cid
