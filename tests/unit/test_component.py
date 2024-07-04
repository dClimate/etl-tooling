from dc_etl import component


def test_fetcher():
    fetcher = component.fetcher("testing", "one", "two", foo="bar", bar="baz")
    assert fetcher.args == ("one", "two")
    assert fetcher.foo == "bar"
    assert fetcher.bar == "baz"


def test_extractor():
    extractor = component.extractor("testing", "one", "two", foo="bar", bar="baz")
    assert extractor.args == ("one", "two")
    assert extractor.foo == "bar"
    assert extractor.bar == "baz"


def test_combiner():
    combiner = component.combiner("testing", "one", "two", foo="bar", bar="baz")
    assert combiner.args == ("one", "two")
    assert combiner.foo == "bar"
    assert combiner.bar == "baz"


def test_combine_preprocessor():
    combine_preprocessor = component.combine_preprocessor("testing", "one", "two", foo="bar", bar="baz")
    assert combine_preprocessor.args == ("one", "two")
    assert combine_preprocessor.foo == "bar"
    assert combine_preprocessor.bar == "baz"


def test_combine_postprocessor():
    combine_postprocessor = component.combine_postprocessor("testing", "one", "two", foo="bar", bar="baz")
    assert combine_postprocessor.args == ("one", "two")
    assert combine_postprocessor.foo == "bar"
    assert combine_postprocessor.bar == "baz"


def test_transformer():
    transformer = component.transformer("testing", "one", "two", foo="bar", bar="baz")
    assert transformer.args == ("one", "two")
    assert transformer.foo == "bar"
    assert transformer.bar == "baz"
