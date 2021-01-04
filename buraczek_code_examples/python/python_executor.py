# running examples with pytest proved to be challenging so custom script was made

class PythonExecutor(object):
    def __init__(self, example_obj):
        self.example_obj = example_obj

    def run(self):
        for key in [item for item in self.example_obj.__dir__() if item.startswith('exercise')]:
            print(f'\033[92m============EXAMPLE: {key}\033[0m')
            getattr(self.example_obj, key)()
