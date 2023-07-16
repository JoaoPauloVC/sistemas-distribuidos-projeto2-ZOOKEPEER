class Mensagem:
    def __init__(self, tipo, key=None, value=None, timestamp=None):
        self.tipo = tipo
        self.key = key
        self.value = value
        self.timestamp = timestamp
        # Outros atributos necessários

    # Implemente os métodos necessários para manipular e serializar as mensagens
