import socket
import threading
from mensagem import Mensagem

class Servidor:
    def __init__(self, ip, porta, ip_lider, porta_lider):
        self.ip = ip
        self.porta = porta
        self.ip_lider = ip_lider
        self.porta_lider = porta_lider
        self.socket_servidor = None
        self.respostas_replicacao = {}  # Dicionário para armazenar respostas de replicação_ok
        self.tabelahash = {}  # Tabela hash local para armazenar as chaves e valores das requisições PUT
        # self.servidores_conectados = []  # Lista para armazenar os endereços dos servidores conectados ao líder



    def inicializar(self):
        self.socket_servidor = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket_servidor.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket_servidor.bind((self.ip, self.porta))
        self.socket_servidor.listen(5)
        print(f"Servidor iniciado em {self.ip}:{self.porta}")
        # Caso não seja o Líder, envia um sinal para o Líder saber para quem deve replicar as mensagens
        if(self.ip != self.ip_lider & self.porta != self.porta_lider):
            # Implementar código que faz com que o Servidor passe seu IP e Porta para o Servidor Líder armazenar na listade servidores conectados
            pass
        self.executar()

    def tratar_requisicoes(self, conexao, endereco):
        while True:
            mensagem_serializada = conexao.recv(1024).decode()
            if not mensagem_serializada:
                break
            
            mensagem = Mensagem.from_json(mensagem_serializada)
            if mensagem.tipo == "PUT":
                if self.ip == self.ip_lider and self.porta == self.porta_lider:
                    self.processar_requisicao_put(mensagem, conexao)
                else:
                    self.encaminhar_put(mensagem)
            elif mensagem.tipo == "REPLICATION":
                self.receber_requisicao_replication(mensagem)
            elif mensagem.tipo == "REPLICATION_OK":
                self.receber_requisicao_replication_ok(mensagem)
            elif mensagem.tipo == "GET":
                self.receber_requisicao_get(mensagem)

        print(f"Conexão encerrada: {endereco}")
    
    def receber_requisicoes(self):
        while True:
            conexao, endereco = self.socket_servidor.accept()
            print(f"Nova conexão estabelecida: {endereco}")
            threading.Thread(target=self.tratar_requisicoes, args=(conexao, endereco)).start()

    
    def encaminhar_put(self, mensagem):

        try:
            conexao_lider = socket.socket()
            conexao_lider.connect((self.ip_lider, self.porta_lider))
            conexao_lider.send(mensagem.to_json().encode())

            # O servidor secundário não espera por uma resposta do líder
            conexao_lider.close()

        except Exception as e:
            print(f"Erro ao encaminhar PUT para o líder: {e}")
    
    def processar_requisicao_put(self, mensagem, conexao_cliente):
        # Processar a requisição PUT e enviar replicação para os servidores secundários
        chave = mensagem.key
        valor = mensagem.value
        self.tabelahash[chave] = valor

        # Enviar a replicação para os servidores secundários
        self.replicar_informacao(mensagem)

        # Enviar resposta PUT_OK para o cliente
        self.enviar_put_ok(chave, mensagem.timestamp)    
    
    def replicar_informacao(self, mensagem):
        if not (self.ip == self.ip_lider and self.porta == self.porta_lider):
            # O servidor não deve replicar informações se não for o líder
            return

        for endereco_servidor in self.endereco_servidores[1:]:  # Exclui o próprio líder da lista
            try:
                conexao_secundario = socket.socket()
                conexao_secundario.connect(endereco_servidor)
                mensagem_replicacao = Mensagem(
                    tipo="REPLICATION",
                    key=mensagem.key,
                    value=mensagem.value,
                    timestamp=mensagem.timestamp,
                    timestamp_servidor=mensagem.timestamp_servidor
                )
                conexao_secundario.send(mensagem_replicacao.to_json().encode())

                # O líder não espera por uma resposta dos servidores secundários
                conexao_secundario.close()

            except Exception as e:
                print(f"Erro ao replicar informação para o servidor secundário: {e}")

    
    def enviar_put_ok(self, mensagem):
        if not (self.ip == self.ip_lider and self.porta == self.porta_lider):
            # Somente o líder deve enviar PUT_OK para o cliente
            return

        try:
            cliente_ip = mensagem.timestamp_servidor
            cliente_porta = mensagem.timestamp
            conexao_cliente = socket.socket()
            conexao_cliente.connect((cliente_ip, cliente_porta))
            mensagem_put_ok = Mensagem(
                tipo="PUT_OK",
                key=mensagem.key,
                value=mensagem.value,
                timestamp=mensagem.timestamp,
                timestamp_servidor=mensagem.timestamp_servidor
            )
            conexao_cliente.send(mensagem_put_ok.to_json().encode())

            # O líder não espera por uma resposta do cliente
            conexao_cliente.close()

        except Exception as e:
            print(f"Erro ao enviar PUT_OK para o cliente: {e}")
    
    def receber_requisicao_replication(self, mensagem):
        self.tabelahash[mensagem.key] = mensagem.value
        # Líder não precisa enviar resposta para o servidor secundário
        print(f"Recebida replicação para a chave '{mensagem.key}'")
    
    def receber_requisicao_replication_ok(self, mensagem):
        # Verificar se a chave da requisição PUT existe no dicionário de respostas_replicacao
        chave = mensagem.key
        if chave not in self.respostas_replicacao:
            self.respostas_replicacao[chave] = set()  # Usamos um conjunto para armazenar as respostas

        # Adicionar a resposta de replicação_ok ao conjunto correspondente
        self.respostas_replicacao[chave].add(mensagem.timestamp_servidor)

        # Verificar se todas as respostas foram recebidas para enviar PUT_OK para o cliente
        if len(self.respostas_replicacao[chave]) == len(self.endereco_servidores) - 1:
            self.enviar_put_ok(chave, mensagem.timestamp)

    
    def receber_requisicao_get(self, mensagem):
        chave = mensagem.key
        if chave in self.tabelahash:
            valor = self.tabelahash[chave]
            resposta = Mensagem(
                tipo="GET_RESPONSE",
                key=chave,
                value=valor,
                timestamp=mensagem.timestamp,
                timestamp_servidor=mensagem.timestamp_servidor
            )
        else:
            resposta = Mensagem(
                tipo="GET_RESPONSE",
                key=chave,
                value=None,
                timestamp=mensagem.timestamp,
                timestamp_servidor=mensagem.timestamp_servidor
            )

        try:
            conexao_cliente = socket.socket()
            conexao_cliente.connect((mensagem.timestamp_servidor, mensagem.timestamp))
            conexao_cliente.send(resposta.to_json().encode())

            # O líder não espera por uma resposta do cliente
            conexao_cliente.close()

        except Exception as e:
            print(f"Erro ao enviar resposta GET para o cliente: {e}")

    
    def executar(self):
        while True:
            conexao, endereco = self.socket_servidor.accept()
            print(f"Nova conexão estabelecida: {endereco}")
            threading.Thread(target=self.tratar_requisicoes, args=(conexao, endereco)).start()
    

if __name__ == "__main__":
    servidor_ip = input("Servidor IP: ")
    servidor_porta = int(input("Servidor Porta: "))
    servidor_ip_lider = input("Servidor Líder IP: ")
    servidor_porta_lider = int(input("Servidor Líder Porta: "))
    servidor = Servidor(servidor_ip, servidor_porta, servidor_ip_lider, servidor_porta_lider)
    servidor.inicializar()