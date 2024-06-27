import numpy


def npdate(year: int, month: int, day: int) -> numpy.datetime64:
    """Helper method to make numpy dates easier to express"""
    return numpy.datetime64(f"{year}-{month:02d}-{day:02d}")
