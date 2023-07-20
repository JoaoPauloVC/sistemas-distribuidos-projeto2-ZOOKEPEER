import json

class Mensagem:
    def __init__(self, tipo, key=None, value=None, timestamp=None, timestamp_servidor=None):
        self.tipo = tipo
        self.key = key
        self.value = value
        self.timestamp = timestamp
        self.timestamp_servidor = timestamp_servidor

    def to_json(self):
        return json.dumps(self.__dict__)

    @classmethod
    def from_json(cls, mensagem_json):
        mensagem_dict = json.loads(mensagem_json)
        return cls(**mensagem_dict)
