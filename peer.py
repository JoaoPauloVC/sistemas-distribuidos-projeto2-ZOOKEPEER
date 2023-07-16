import socket

from mensagem import Mensagem


class Peer:
    def __init__(self):
        self.ip_servidores = []  # Insira os IPs dos servidores
        self.portas_servidores = []  # Insira as portas dos servidores
        self.conexoes = []  # Armazena as conexões estabelecidas

        # Inicialize outras variáveis e estruturas necessárias

    def inicializar(self):
        num_servidores = 3  # Número de servidores

        for i in range(num_servidores):
            while True:
                try:
                    ip = input(f"Informe o IP do servidor {i+1}: ")
                    porta = int(input(f"Informe a porta do servidor {i+1}: "))

                    self.ip_servidores.append(ip)
                    self.portas_servidores.append(porta)

                    break
                except ValueError:
                    print("Por favor, insira um valor válido para a porta.")
        print("Inicializar Chegou Aqui")

        # Implemente outras etapas de inicialização, se necessário

    
    def enviar_requisicao_put(self, key, value):
        for conexao in self.conexoes:
            try:
                mensagem = Mensagem("PUT", key=key, value=value)
                mensagem_serializada = mensagem.to_json()
                
                # Envie a mensagem serializada através da conexão com o servidor
                conexao.send(mensagem_serializada.encode())

                # Receba a resposta do servidor
                resposta_serializada = conexao.recv(1024).decode()

                # Realize o tratamento da resposta do servidor
                resposta = Mensagem.from_json(resposta_serializada)
                if resposta.tipo == "PUT_OK":
                    # Exiba os resultados
                    print(f"PUT_OK key: {key} value: {value} timestamp: {resposta.timestamp} realizada no servidor {conexao.getpeername()}")

                # Lembre-se de fechar a conexão após o uso
                conexao.close()

            except Exception as e:
                print(f"Erro ao enviar a requisição PUT: {e}")
        print("PUT Chegou Aqui")
        

    
    def enviar_requisicao_get(self, key):
        for conexao in self.conexoes:
            try:
                mensagem = Mensagem("GET", key=key)
                mensagem_serializada = mensagem.to_json()

                # Envie a mensagem serializada através da conexão com o servidor
                conexao.send(mensagem_serializada.encode())

                # Receba a resposta do servidor
                resposta_serializada = conexao.recv(1024).decode()

                # Realize o tratamento da resposta do servidor
                resposta = Mensagem.from_json(resposta_serializada)
                if resposta.tipo == "GET_RESPONSE":
                    # Exiba os resultados
                    print(f"GET key: {key} value: {resposta.value} obtido do servidor {conexao.getpeername()}, meu timestamp {resposta.timestamp} e do servidor {resposta.timestamp_servidor}")

                # Lembre-se de fechar a conexão após o uso
                conexao.close()

            except Exception as e:
                print(f"Erro ao enviar a requisição GET: {e}")
        print("GET Chegou Aqui")
    
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
    diretorio = input("Pasta onde os arquivos estão armazenados: ")
    
    # Configurar as informações do peer
    peer.ip_servidores = [peer_ip]  # Insira os IPs dos servidores
    peer.portas_servidores = [peer_porta]  # Insira as portas dos servidores
    # Outras configurações do peer, se necessário
    
    peer.executar()

