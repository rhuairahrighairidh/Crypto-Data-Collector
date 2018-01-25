import os
import importlib

script_name = os.environ['SCRIPT']
script_module = importlib.import_module('scripts.'+script_name)

script_module.main()
