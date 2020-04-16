
from pathlib import Path
import yaml

stream = open(Path.joinpath(Path(__file__).parent, "variables.yaml"))
config = yaml.load(stream, Loader=yaml.FullLoader)
