from dc_etl import transform as transform_module


def test_identity():
    apple = object()
    assert transform_module.identity(apple) is apple
