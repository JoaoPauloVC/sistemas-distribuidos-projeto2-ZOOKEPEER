import socket
from mensagem import Mensagem
import random


class Peer:
    def __init__(self):
        self.endereco_servidores = [] #Armazena as conexões com os servidores
        self.tabelahash = {}
        # Inicialize outras variáveis e estruturas necessárias

    # Seção 4.a - Inicialização do Peer
    def inicializar(self):
        num_servidores = 2  # Número de servidores

        for i in range(num_servidores):
            while True:
                try:
                    ip = input(f"Informe o IP do servidor {i+1}: ")
                    porta = int(input(f"Informe a porta do servidor {i+1}: "))

                    self.endereco_servidores.append((ip, porta))                    

                    break
                except ValueError:
                    print("Por favor, insira um valor válido para a porta.")

    # Seção 4.b - Envio do PUT
    def enviar_requisicao_put(self, key, value):
        servidor_escolhido = random.choice(self.endereco_servidores)
        try:
            mensagem = Mensagem("PUT", key=key, value=value)
            mensagem_serializada = mensagem.to_json()
            
            # Envia a mensagem serializada através da conexão com o servidor
            conexao_server = socket.socket()
            conexao_server.connect(servidor_escolhido)
            conexao_server.send(mensagem_serializada.encode())
            
            # conexao_server.close()

            # Resposta do servidor
            resposta_serializada = conexao_server.recv(1024).decode()

            # Tratamento da resposta do servidor
            resposta = Mensagem.from_json(resposta_serializada)
            if resposta.tipo == "PUT_OK":
                print(f"PUT_OK key: {key} value: {value} timestamp: {resposta.timestamp} realizada no servidor {endereco_servidores.getpeername()}")

            conexao_server.close()

        except Exception as e:
            print(f"Erro ao enviar a requisição PUT: {e}")
        
    def enviar_requisicao_get(self, key):
        servidor_escolhido = random.choice(self.endereco_servidores)
        try:
            mensagem = Mensagem("GET", key=key)
            mensagem_serializada = mensagem.to_json()

            # Envia a mensagem serializada para o servidor escolhido
            conexao_server = socket.socket()
            conexao_server.connect(servidor_escolhido)
            conexao_server.send(mensagem_serializada.encode())

            # Resposta do servidor
            resposta_serializada = conexao_server.recv(1024).decode()

            # Tratamento da resposta do servidor
            resposta = Mensagem.from_json(resposta_serializada)
            if resposta.tipo == "GET_RESPONSE":
                print(f"GET key: {key} value: {resposta.value} obtido do servidor {servidor_escolhido}, meu timestamp {resposta.timestamp} e do servidor {resposta.timestamp_servidor}")

            conexao_server.close()

        except Exception as e:
            print(f"Erro ao enviar a requisição GET: {e}")
    
    def exibir_menu(self):
        while True:
            print("\nMenu Interativo")
            print("1. Inicializar")
            print("2. PUT")
            print("3. GET")
            print("4. Sair")
            
            opcao = input("Escolha uma opção: ")
            
            if opcao == "1":
                self.inicializar()
            elif opcao == "2":
                key = input("Digite a chave: ")
                value = input("Digite o valor: ")
                self.enviar_requisicao_put(key, value)
            elif opcao == "3":
                key = input("Digite a chave: ")
                self.enviar_requisicao_get(key)
            elif opcao == "4":
                break
            else:
                print("Opção inválida. Por favor, escolha uma opção válida.")

    
    def executar(self):
        # Implemente a lógica principal do peer
        self.exibir_menu()
    
# Seção para inicialização do peer
if __name__ == "__main__":
    peer = Peer()
    peer_ip = input("Peer IP: ")
    peer_porta = int(input("Peer Porta: "))
    
    # Configurar as informações do peer
    peer.ip_servidores = [peer_ip]  # Insira os IPs dos servidores
    peer.portas_servidores = [peer_porta]  # Insira as portas dos servidores
    # Outras configurações do peer, se necessário
    
    peer.executar()

