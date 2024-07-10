import yaml

from . import filespec

yaml.add_constructor("!filespec", filespec.File._load_yaml)
