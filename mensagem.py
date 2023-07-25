import json

class Mensagem:
    def __init__(self, tipo, conteudo):
        self.tipo = tipo
        self.conteudo = conteudo


    def to_json(self):
        return json.dumps(self, default=lambda o: o.__dict__)

    @classmethod
    def from_json(cls, mensagem_json):
        mensagem_dict = json.loads(mensagem_json)
        return cls(**mensagem_dict)
