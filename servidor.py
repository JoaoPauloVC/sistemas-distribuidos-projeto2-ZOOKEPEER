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

    def inicializar(self):
        self.socket_servidor = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket_servidor.bind((self.ip, self.porta))
        self.socket_servidor.listen(5)
        print(f"Servidor iniciado em {self.ip}:{self.porta}")
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
        # Implemente a lógica para receber as requisições dos clientes
        # Dica: utilize um loop para ficar ouvindo por novas requisições
        pass
    
    def encaminhar_put(self, mensagem):
        # Implemente a lógica para encaminhar uma requisição PUT para o líder
        pass
    
    def replicar_informacao(self, mensagem):
        # Implemente a lógica para replicar uma informação nos outros servidores
        pass
    
    def enviar_put_ok(self, mensagem):
        # Implemente a lógica para enviar uma mensagem PUT_OK para o cliente
        pass
    
    def receber_requisicao_replication(self, mensagem):
        # Implemente a lógica para tratar uma requisição de replicação
        pass
    
    def receber_requisicao_replication_ok(self, mensagem):
        # Implemente a lógica para tratar uma requisição de replicação_ok
        pass
    
    def receber_requisicao_get(self, mensagem):
        # Implemente a lógica para tratar uma requisição GET
        pass
    
    def executar(self):
        while True:
            conexao, endereco = self.socket_servidor.accept()
            print(f"Nova conexão estabelecida: {endereco}")
            threading.Thread(target=self.tratar_requisicoes, args=(conexao, endereco)).start()
    
# Seção para inicialização do servidor Líder
ip_lider = '127.0.0.1'  # Insira o IP do líder
porta_lider = 10097  # Insira a porta do líder

if __name__ == "__main__":
    servidor_ip = input("Servidor IP: ")
    servidor_porta = int(input("Servidor Porta: "))

    servidor = Servidor(servidor_ip, servidor_porta, ip_lider, porta_lider)
    servidor.inicializar()