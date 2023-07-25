import socket
from mensagem import Mensagem
import random


class Peer:
    
    # Atributos da Classe
    def __init__(self):
        self.endereco_servidores = [] #Armazena as conexões com os servidores
        self.tabelahash = {} # Tabela hash

    # Seção 6 (Peer) - Menu Interativo
    def exibir_menu(self):
        
        # Forçar Inicialização do Peer
        while True:
            opcao = input("Aperte 1 para Inicializar o Peer:")
            if (opcao == "1"):
                break
        
        # Seção 4.a - Inicialização do Peer
        self.inicializar()

        # Menu Interativo
        while True:        
            print("\nMenu Interativo")
            print("1. PUT")
            print("2. GET")
            
            opcao = input("Escolha uma opção: ")
            
            # Requisição PUT
            if opcao == "1":
                chave = input("Digite a chave: ")
                valor = input("Digite o valor: ")
                self.enviar_requisicao_put(chave, valor)
            
            # Requisição GET
            elif opcao == "2":
                chave = input("Digite a chave: ")
                self.enviar_requisicao_get(chave)
            


    # Seção 4.a - Inicialização do Peer
    def inicializar(self):
        num_servidores = 3  # Número de servidores

        # Espera por IP e Porta do num_servidores
        for i in range(num_servidores):
            ip = input(f"Informe o IP do servidor {i+1}: ")
            porta = int(input(f"Informe a porta do servidor {i+1}: "))
            
            # Adiciona na lista endereco_servidores
            self.endereco_servidores.append((ip, porta))  
                    
    # Seção 4.b - Envio do PUT
    def enviar_requisicao_put(self, chave, valor):
        
        # Cria e serializa Mensagem GET que será enviada    
        mensagem = Mensagem('PUT', (chave, valor))
        mensagem_serializada = mensagem.to_json().encode()
        
        # Escolher Servidor aleatoriamente (dentro da lista de endereco_servidores)
        servidor_escolhido = random.choice(self.endereco_servidores)
        
        # Envia a mensagem serializada através da conexão com o servidor
        conexao_server = socket.socket()
        conexao_server.connect(servidor_escolhido)
        conexao_server.send(mensagem_serializada)
        
        # Resposta do servidor e desserialização dela
        resposta_serializada = conexao_server.recv(1024).decode()
        resposta = Mensagem.from_json(resposta_serializada)
        
        # Fechando conexão com o Servidor
        conexao_server.close()

        # Tratamento da Resposta PUT
        if resposta.tipo == "PUT_OK":
            self.tabelahash.update({chave:( valor, resposta.conteudo)})

            # Seção 6 (Peer) - Print PUT_OK
            print(f"PUT_OK key: {chave} value: {valor} timestamp: {resposta.conteudo} realizada no servidor {servidor_escolhido[0]}:{servidor_escolhido[1]}")

    # Seção 4.c - Envio da Requisicação GET     
    def enviar_requisicao_get(self, chave):
        
        # Verifica se a chave já existe na tabela Hash e pega a timestamp associada, ou cria ela.
        if chave in self.tabelahash:
            timestamp = self.tabelahash[chave][1]
        else:
            timestamp = 0
        
        # Cria e serializa Mensagem GET que será enviada    
        mensagem = Mensagem("GET", (chave, timestamp))
        mensagem_serializada = mensagem.to_json().encode()


        # Escolher Servidor aleatoriamente (dentro da lista de endereco_servidores)
        servidor_escolhido = random.choice(self.endereco_servidores)
        
        # Envia a mensagem serializada para o servidor escolhido
        conexao_server = socket.socket()
        conexao_server.connect(servidor_escolhido)
        conexao_server.send(mensagem_serializada)

        # Resposta do servidor e desserialização dela
        resposta_serializada = conexao_server.recv(1024).decode()
        resposta = Mensagem.from_json(resposta_serializada)
        
        # Fechando conexão com o Servidor
        conexao_server.close()
        
        # Tratamento da Resposta GET
        if resposta.tipo == 'GET_OK':
            self.tabelahash.update({chave:resposta.conteudo})
            print(f'GET key: {chave} value: {resposta.conteudo[0]} obtido do servidor {servidor_escolhido[0]}:{servidor_escolhido[1]}, meu timestamp {timestamp} e do servidor {resposta.conteudo[1]}')
        elif resposta.tipo == 'NULL':
            print('NULL')
        else:
            print('TRY_OTHER_SERVER_OR_LATER')
            
 
# Seção para inicialização do peer
if __name__ == "__main__":
    peer = Peer()
    peer.exibir_menu()

