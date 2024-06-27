import yaml

from . import filespec

yaml.add_constructor("!filespec", filespec.FileSpec._load_yaml)
