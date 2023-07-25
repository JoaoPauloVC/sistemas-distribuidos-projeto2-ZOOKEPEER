import socket
import time
import threading
from mensagem import Mensagem

class Servidor:
    
    # Atribuitos da Classe
    def __init__(self, endereco_servidor, endereco_lider):
        self.ip = endereco_servidor[0]
        self.porta = endereco_servidor[1]
        self.endereco_servidor = endereco_servidor
        self.ip_lider= endereco_lider[0]
        self.porta_lider = endereco_lider[1]
        self.endereco_lider = endereco_lider
        self.tabelahash = {}  # Tabela hash local para armazenar as chaves e valores das requisições PUT
        self.servidores_conectados = []  # Lista para armazenar os endereços dos servidores conectados ao líder

    # Seção 5.a - Parte 2 - Inicialização do Servidor
    def inicializar(self):
        self.socket_servidor = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        
        # Para fechar o socket ao encerrar o programa.
        self.socket_servidor.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        
        self.socket_servidor.bind((self.ip, self.porta))
        self.socket_servidor.listen(5)
        
        # Caso não seja líder, envie endereço próprio para o líder armazenar
        if not (self.eh_lider()):
            self.enviar_servidor_join()

        self.executa_thread()
   
    # Seção 5.b - Recebe e responde simultaneamente (com uso de Threads).            
    def executa_thread(self):
        while True:
            conexao, _ = self.socket_servidor.accept()
            endereco_peer = conexao.getpeername()
            threading.Thread(target=self.tratar_requisicoes, args=(conexao, endereco_peer)).start()
               
    ## AUXILIAR -> Trata os tipos de Requisições.
    def tratar_requisicoes(self, conexao, endereco_peer):
        
        mensagem_serializada = conexao.recv(1024).decode()
        mensagem = Mensagem.from_json(mensagem_serializada)
        
        # Tipos de requisição
        if mensagem:
            # Seção 5.c - Recebe PUT e Envia Resposta para o Peer
            if mensagem.tipo == "PUT":
                # Timestamp para enviar para o Peer
                timestamp = self.processar_requisicao_put(mensagem.conteudo, endereco_peer)
                
                ####
                # Enviar valor de timestamp original p/ peer (No caso de ser porta par -> 10098). 
                if(self.porta%2 == 0):
                    timestamp = timestamp*2
                    
                # Seção 5.e - Líder envia PUT_OK para o Peer
                resposta = Mensagem('PUT_OK', timestamp)
                resposta_serializada = resposta.to_json().encode()
                conexao.send(resposta_serializada) 
            
            # Seção 5.d - Recebe REPLICATION
            elif mensagem.tipo == "REPLICATION":
                (chave, valor, timestamp) = mensagem.conteudo
                
                ####
                # TimeStamp é metade do original em Servidores de porta par (10098). Para forçar a mensagem TRY_ANOTHER_SERVER_OR_LATER
                if(self.porta%2 == 0):
                    timestamp = float(timestamp/2)

                # Servidor atualizar sua própria tabela Hash
                self.tabelahash.update({chave:(valor, timestamp)})
                
                # Print do console (REPLICATION)
                print(f"REPLICATION key:{chave} value:{valor} ts:{timestamp}")
                
                # Cria e envia a mensagem de REPLICATION_OK para o Líder. 
                resposta = Mensagem('REPLICATION_OK', None)
                resposta_serializada = resposta.to_json().encode()
                conexao.send(resposta_serializada)

            # Seção 5.f - Recebe GET
            elif mensagem.tipo == "GET":
                resultado = self.receber_requisicao_get(mensagem.conteudo)
                (chave, valor) = mensagem.conteudo
                
                self.retorno_get(resultado, conexao, endereco_peer, mensagem.conteudo)
         
            ## AUXILIAR -> Adiciona servidor à lista de servidores_conectados ao Líder
            elif mensagem.tipo == "SERVIDOR_JOIN":
                self.servidores_conectados.append(tuple(mensagem.conteudo))
        
        conexao.close()
        return

    # Seção 5.c - Trata Requisição PUT
    def processar_requisicao_put(self, conteudo, endereco_peer):
        (chave, valor) = conteudo
        (peer_ip, peer_porta) = endereco_peer
        
        # Seção 5.c - Caso Não Líder
        if not self.eh_lider():
            # Print PUT Não Líder
            print(f"Encaminhando PUT key:{chave} value:{valor}")
            timestamp = self.encaminhar_put_para_lider(conteudo)
            
            ####
            # Forçando erro no servidor de porta par (10098)
            if(self.porta%2 == 0):
                timestamp = float(timestamp/2)
             
        # Seção 5.c - Caso Líder                  
        else:
            # Print PUT Líder
            print(f"Cliente {peer_ip}:{peer_porta} PUT key:{chave} value:{valor}")
            # Atualiza Timestamp com a hora
            timestamp = time.time()
            
            self.replicar(chave, valor, timestamp)

            
            print(f"Enviando PUT_OK ao Cliente {peer_ip}:{peer_porta} da key:{chave} ts:{timestamp}")
                              
            # Seção 5.c.1 - Líder atualiza sua tabela Hash
            self.tabelahash.update({chave:(valor, timestamp)})
        
        
        return timestamp

    # Seção 5.c - Caso Não Líder
    def encaminhar_put_para_lider(self, conteudo):
        
        # Cria e serializa a mensagem para o Líder (repassando a mensagem)
        mensagem_put_para_lider = Mensagem("PUT", conteudo)
        mensagem_serializada = mensagem_put_para_lider.to_json().encode()
        
        # Cria conexão e envia PUT para o Líder
        conexao_lider = socket.socket()
        conexao_lider.connect(self.endereco_lider)
        conexao_lider.send(mensagem_serializada)
        
        # Seção 5.d Recebe REPLICATION do Líder
        resposta_serializada = conexao_lider.recv(1024).decode()
        resposta = Mensagem.from_json(resposta_serializada)
        
        # Fecha conexão com lider
        conexao_lider.close()
        
        # TimeStamp retornado pelo Líder        
        timestamp = resposta.conteudo
        
        return timestamp        
    
    # Líder envia REPLICATION e recebe REPLICATION_OK
    def replicar(self, chave, valor, timestamp):
        for endereco_servidor in self.servidores_conectados:
                # Líder Cria conexão com os servidores_conectados
                conexao_servidor = socket.socket()
                conexao_servidor.connect(endereco_servidor)
                
                # Seção 5.c.2 - Cria e envia Mensagem serializada de replicação para os servidores
                mensagem_replicacao = Mensagem('REPLICATION', [chave, valor, timestamp])
                mensagem_replicacao_serializada = mensagem_replicacao.to_json().encode()
                conexao_servidor.send(mensagem_replicacao_serializada)

                # Seção 5.e - Recebe REPLICATION_OK dos Servidores
                resposta_serializada = conexao_servidor.recv(1024).decode()
                resposta = Mensagem.from_json(resposta_serializada)
                
                # Assegura que o tipo da resposta seja "REPLICATION_OK"  
                assert resposta.tipo == 'REPLICATION_OK'   
                     
    # Seção 5.f - Trata Requisição GET                
    def receber_requisicao_get(self, conteudo):
        (chave, timestamp) = conteudo
        
        # Se a chave existe
        if chave in self.tabelahash:
            # Se o TimeStamp do Peer é menor ou igual ao da Tabela Hash do Servidor
            if timestamp <= self.tabelahash[chave][1]:
                return self.tabelahash[chave]                
            else:
                return "TRY_ANOTHER_SERVER_OR_LATER"
            
        # Caso Não Exista
        else:
            return None

    # AUXILIAR -> Mensagem de retorno do GET
    def retorno_get(self, resultado, conexao, endereco_peer, mensagem):
        (peer_ip, peer_porta) = endereco_peer
        (chave, valor) = mensagem
        if isinstance(resultado, tuple):
            resposta = Mensagem('GET_OK', resultado)
            resposta_serializada = resposta.to_json().encode()
            conexao.send(resposta_serializada)
            print(f"Cliente {peer_ip}:{peer_porta} GET key:{chave} ts:{valor}. Meu ts é {self.tabelahash[chave][1]}, portanto devolvendo {self.tabelahash[chave][0]}")
        elif resultado == "TRY_ANOTHER_SERVER_OR_LATER":
            resposta = Mensagem('ERRO', resultado)
            resposta_serializada = resposta.to_json().encode()
            conexao.send(resposta_serializada)
            print(f"Cliente {peer_ip}:{peer_porta} GET key:{chave} ts:{valor}. Meu ts é {self.tabelahash[chave][1]}, portanto devolvendo TRY_ANOTHER_SERVER_OR_LATER")
        else:
            resposta = Mensagem('NULL', resultado)
            resposta_serializada = resposta.to_json().encode()
            conexao.send(resposta_serializada)
            print(f"Cliente {peer_ip}:{peer_porta} GET key:{chave} ts:{valor}. Meu ts é 0, portanto devolvendo NULL")
    
    ## AUXILIAR -> Bool True = Líder; False = Não_líder
    def eh_lider(self):
        return (self.ip == self.ip_lider and self.porta == self.porta_lider)

    ## AUXILIAR -> Servidor envia mensagem para conectar com o Servidor Líder e passar seu endereço
    def enviar_servidor_join(self):
            mensagem = Mensagem("SERVIDOR_JOIN", self.endereco_servidor)
            mensagem_serializada = mensagem.to_json().encode()
            
            # Envia a mensagem serializada através da conexão com o servidor
            conexao_lider = socket.socket()
            conexao_lider.connect(self.endereco_lider)
            conexao_lider.send(mensagem_serializada)

            conexao_lider.close()
           
if __name__ == "__main__":
    
    # Seção 5.a - Parte 1 - Captura de Dados
    servidor_ip = input("Servidor IP: ")
    servidor_porta = int(input("Servidor Porta: "))
    servidor_ip_lider = input("Servidor Líder IP: ")
    servidor_porta_lider = int(input("Servidor Líder Porta: "))
    servidor = Servidor((servidor_ip, servidor_porta), (servidor_ip_lider, servidor_porta_lider))
    # Seção 5.a - Parte 2- Inicialização do Servidor
    servidor.inicializar()