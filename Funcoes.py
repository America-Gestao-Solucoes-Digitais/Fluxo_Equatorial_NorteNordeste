import pandas as pd
import warnings
import shutil
import tempfile
import time as t
import os
from datetime import datetime
from pandas.tseries.offsets import MonthBegin
import sys
from twocaptcha import TwoCaptcha
from selenium.webdriver.chrome.options import Options
import undetected_chromedriver as uc
import mysql.connector
from sqlalchemy import create_engine
import urllib.parse

warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=UserWarning)


# Classe que define a conexão com o banco sql da américa energia
class DatabaseConfig:
    
    # Começa com as credenciais para entrar no banco de dados
    def __init__(self, username, password):
        self.server = 'gestaodb2.americaenergia.com.br'
        self.database = 'america_gestao'
        self.username = username
        self.password = password
        self.port = 3306

    # Após a inserção dos dados de login será feito a conexão na rede com o sql    
    def connect(self):
        
        try:
            # Codifica a senha para URL
            encoded_password = urllib.parse.quote_plus(self.password)
            
            # Constrói a URL de conexão
            connection_url = (
                f"mysql+mysqlconnector://{self.username}:{encoded_password}"
                f"@{self.server}:{self.port}/{self.database}")
            
            # Cria e retorna o engine do SQLAlchemy
            engine = create_engine(connection_url)
            return engine
        
        except Exception as e:
            print()
            print(f"Erro ao conectar: {e}")
            print()
            return None




# Classe nova para inserir o status de coleta e de pagamento das faturas 
class Banco: 
    

    # Método para iniciar com a conexão do banco de dados do acces 
    def __init__(self, conn):
        self.conn = conn


    # Método para iniciar com a inserção de dados no banco quando acontece um erro em alguma etapa do fluxo
    def Processar(self,UC,Cliente,status_erro,dist,referencia):
        self.inserir_status(UC,Cliente,dist,referencia,status_erro)


    # Método que cria o status e insere no banco de dados do acces para dizer se a fatura da uc naquele mes foi emitida ou não
    def inserir_status(self,UC,Cliente,Dist,referencia, status_fatura):
        cursor = self.conn.cursor()
        data_execucao = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        query_insert = "INSERT INTO tbl_status (uc_fatura, cliente_fatura, dist_fatura, data_referencia, data_vencimento, data_execucao, status_fatura) VALUES (?, ?, ?, ?, ?, ?, ?)"
        valores = (UC, Cliente, Dist,referencia,"01/01/1900",data_execucao,status_fatura)
        cursor.execute(query_insert, valores)
        self.conn.commit()


    # Método que insere o status de login no banco acces para verificar se há algum acesso que precisa ser regularizado ou não
    def inserir_status_login(self, login,senha, cpf, Cliente, Dist, status_login):
        cursor = self.conn.cursor()
        data_execucao = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        query_insert = "INSERT INTO tbl_logins (login, senha, passo_adicional, cliente_login, distribuidora_login, data_execucao, status_login) VALUES (?, ?, ?, ?, ?, ?, ?)"
        valores = (login, senha, cpf, Cliente, Dist, data_execucao,status_login)
        cursor.execute(query_insert,valores)
        self.conn.commit()


    # Método que insere o status de pagamento de cada fatura
    def inserir_pagamento(self, Tabela, UC, Cliente, Dist, Modalidade):

        
        

        # Extrai os dados de unidade consumidora, cliente, distribuidora e modalidade da tabela
        Tabela['UC']            = UC
        Tabela['Cliente']       = Cliente
        Tabela['Dist']          = Dist
        Tabela['Moalidade']     = Modalidade
 

        # Condição para CEB
        if Dist == "CEB":
            for _, row in Tabela.iterrows():
                cursor = self.conn.cursor()
                data_execucao = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                data_hoje = datetime.now().strftime('%Y-%m-%d')
                data_hoje_dt = datetime.strptime(data_hoje, "%Y-%m-%d")
                data_vencimento = datetime.strptime(row["Vencimento"], "%d/%m/%Y")
    
                if Modalidade == "BT":
                    if "/" in row["Data Pagamento"]:
                        status_pagamento = "Paga"
                    elif (data_vencimento < data_hoje_dt) and ("/" not in row["Data Pagamento"]): 
                        status_pagamento = "Vencida"
                    else: 
                        status_pagamento = "Em aberto"
                    
                    
                    query_insert = "INSERT INTO tbl_pagamentos (uc_fatura, cliente_fatura, dist_fatura, data_referencia, data_vencimento, data_execucao, status_fatura) VALUES (?, ?, ?, ?, ?, ?, ?)"
                    valores = (row["UC"], row["Cliente"], row["Dist"], "01/" + row["Fatura"], row["Vencimento"], data_execucao, status_pagamento)
                    cursor.execute(query_insert, valores)
                    self.conn.commit()
                
                elif Modalidade == "MT":
                    if "/" in row["Pagamento"]:
                        status_pagamento = "Paga"
                    elif (data_vencimento < data_hoje_dt) and ("/" not in row["Pagamento"]): 
                        status_pagamento = "Vencida"
                    else: 
                        status_pagamento = "Em aberto"
                    
                    query_insert = "INSERT INTO tbl_pagamentos (uc_fatura, cliente_fatura, dist_fatura, data_referencia, data_vencimento, data_execucao, status_fatura) VALUES (?, ?, ?, ?, ?, ?, ?)"
                    valores = (row["UC"], row["Cliente"], row["Dist"], "01/" + row["Fatura"], row["Vencimento"], data_execucao, status_pagamento)
                    cursor.execute(query_insert, valores)
                    self.conn.commit()
        
        
        
        # Condição para EQUATORIAL GO
        elif Dist == "EQUATORIAL GO":
            
            for _, row in Tabela.iterrows():
                cursor = self.conn.cursor()
                data_execucao = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                data_hoje = datetime.now().strftime('%Y-%m-%d')
                data_hoje_dt = datetime.strptime(data_hoje, "%Y-%m-%d")
                data_vencimento = datetime.strptime(row["Vencimento"], "%d/%m/%Y")
                
                if "/" in row["Pagamento"]:
                    status_pagamento = "Paga"


                # Credenciais para conexão com o sql heidi
                local = 'gestaodb2.americaenergia.com.br'
                usuario = 'america_gestao'
                senha = 'HTHhdt6352s!23'
                bancodedados = 'america_gestao'


                conexao = mysql.connector.connect(
                host=local,
                user=usuario,
                password=senha,
                database=bancodedados
                )

    
                cursor_sql = conexao.cursor(buffered=True)

                data_referencia = datetime.strptime("01/" + row["Mês / Ano"], "%d/%m/%Y").strftime('%Y-%m-%d')
                data_vencimento = datetime.strptime(row["Vencimento"], "%d/%m/%Y").strftime('%Y-%m-%d')

                check_query = """
                    SELECT COUNT(*)
                    FROM tb_status_pagamento_gestao_faturas
                    WHERE INSTALACAO = %s
                    AND REFERENCIA = %s
                    AND DT_VENCIMENTO = %s
                    AND STATUS_PAGAMENTO = %s
                """
                
                cursor_sql.execute(check_query, (UC, data_referencia, data_vencimento, status_pagamento))
                result = cursor_sql.fetchone()[0]

                if result == 0:

                    query = """
                        INSERT INTO tb_status_pagamento_gestao_faturas 
                        (INSTALACAO, REFERENCIA, DT_VENCIMENTO, STATUS_PAGAMENTO, DATA_STATUS, DISTRIBUIDORA)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    """
                    agora = datetime.now()
                    valores_sql = (UC, data_referencia, data_vencimento, status_pagamento, agora, Dist)

                    cursor_sql.execute(query, valores_sql)
                    conexao.commit()

                cursor_sql.close()
                conexao.close()

                query_insert = "INSERT INTO tbl_pagamentos (uc_fatura, cliente_fatura, dist_fatura, data_referencia, data_vencimento, data_execucao, status_fatura) VALUES (?, ?, ?, ?, ?, ?, ?)"
                valores = (row["UC"], row["Cliente"], row["Dist"], "01/" + row["Mês / Ano"], row["Vencimento"], data_execucao, status_pagamento)
                cursor.execute(query_insert, valores)
                self.conn.commit()


    # Função que realiza o movimento das faturas para as pastas de download
    def mover_arquivos_baixados(self, response, mes, ano, cliente, distribuidora, uc, modalidade):
        
        
        # Diretório temporário para salvar o arquivo antes de mover
        temp_dir = tempfile.gettempdir()  
        nome_arquivo_temp = f"{ano}.{mes}_DIST_{distribuidora}_{uc}_{modalidade}.pdf"
        caminho_temp_arquivo = os.path.join(temp_dir, nome_arquivo_temp)

        try:
            # Salva o arquivo PDF baixado
            with open(caminho_temp_arquivo, "wb") as file:
                file.write(response.content)
            print(f"\n- Arquivo temporário salvo: {caminho_temp_arquivo}")

            # Define o caminho final para salvar o arquivo
            pasta_destino = os.path.join(fr'G:\QUALIDADE\Códigos\Leitura de Faturas {modalidade}',cliente,'Faturas')
            t.sleep(3)

            # Cria a pasta destino se não existir
            os.makedirs(pasta_destino, exist_ok=True)

            # Caminho final do arquivo
            caminho_final_arquivo = os.path.join(pasta_destino, nome_arquivo_temp)

            # Move o arquivo para a pasta correta
            shutil.move(caminho_temp_arquivo, caminho_final_arquivo)
            print(f"\n- Arquivo {nome_arquivo_temp} encaminhado para a pasta {caminho_final_arquivo}")

        except Exception as e:
            print(f"\nErro ao mover o arquivo: {str(e)}")


# Função que faz a leitura do banco de dados do sql
def read_table(table_name, engine, columns=None, where=None):
    """
    Lê uma tabela do banco de dados usando pandas
    
    Args:
        table_name (str): Nome da tabela
        engine: Conexão com o banco de dados
        columns (list, optional): Lista de colunas para selecionar. Se None, retorna todas as colunas
        where (str, optional): Condição para filtrar os dados
    """
    query = (
        f"SELECT {', '.join(columns)} "
        f"FROM {table_name} "
    )
    if where:
        query += f" WHERE {where}"

    df = pd.read_sql(query, engine)
    return df


# Antiga função que realiza a consulta no banco de logins
def acessar_logins(conn):
    logins = pd.read_sql('SELECT * FROM Logins', conn)
    return logins


# Antiga função que realiza a chamada para a API do recaptcha
def call_recaptcha_api(api_key):
    solver = TwoCaptcha(api_key)
    return solver


# Antiga função que realiza o recaptcha a partir da API chamada na função call_recaptcha_api
def recaptcha(solver, recaptcha_key, url_site):
    try:
        result = solver.recaptcha(
            sitekey=recaptcha_key,
            url=url_site
        )
        captcha_response = result['code']
        return captcha_response
    except Exception as e:
        print(e)
        sys.exit(e)


# Função que verifica se foi coletada a fatura ou não
def verificacao_coleta(conn, uc_fatura, data_referencia):
    cursor = conn.cursor()
    query_insert = f"""SELECT COUNT(*) FROM tbl_status WHERE uc_fatura = ? AND data_referencia = ? AND status_fatura = 'Baixado'"""
    cursor.execute(query_insert, (uc_fatura, data_referencia))
    result = cursor.fetchone()
    cursor.close()
    if result[0] > 0:
        return True
    else:
        return False



# Função que filtra os clientes conforme está descrito no código main.py
def filtrar_clientes(logins, dist):
    logins_filtrados = logins[(logins["DISTRIBUIDORA"] == dist)]
    return logins_filtrados


# Antiga função que configura o navegador undetectable chrome
def configurar_navegador_uc(pasta_temporaria):
    warnings.filterwarnings("ignore")
    opcoes = uc.ChromeOptions()  # Usando as opções do undetected-chromedriver
    opcoes.add_argument('--start-maximized')
    opcoes.add_argument('--clear-browser-cache')
    opcoes.add_argument("--no-sandbox")
    opcoes.add_argument("--ignore-certificate-errors")
    opcoes.add_argument("--disable-web-security")
    opcoes.add_argument("--disable-infobars")
    opcoes.add_argument("--disable-extensions")
    opcoes.add_argument("--disable-popup-blocking")
    opcoes.add_argument("--disable-notifications")
    
    # Definindo preferências de download e cache
    opcoes.add_experimental_option("prefs", {
        "download.default_directory": pasta_temporaria,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "plugins.always_open_pdf_externally": True,
        'safebrowsing.enabled': True,
        'browser.cache.disk.enable': False,
        'network.cookie.cookieBehavior': 2,  # Bloquear cookies
        'credentials_enable_service': False,
        'profile.password_manager_enabled': False
    })
    return opcoes


# Antiga função que configura o navegador para excluir cookies, processos automaticos, serviços de segurança e entre outros 
def configurar_navegador(pasta_temporaria):
    warnings.filterwarnings("ignore")
    opcoes = Options()
    opcoes.add_argument('--start-maximized')
    opcoes.add_experimental_option("excludeSwitches", ["enable-automation"])
    opcoes.add_experimental_option('useAutomationExtension', False)
    opcoes.add_experimental_option("excludeSwitches", ['enable-logging'])
    opcoes.add_argument('--clear-browser-cache')
    opcoes.add_argument("--disable-automation")
    opcoes.add_argument("--no-sandbox")
    opcoes.add_argument("--ignore-certificate-errors")
    opcoes.add_argument("--disable-web-security")
    opcoes.add_argument("--disable-infobars")
    opcoes.add_argument("--disable-extensions")
    opcoes.add_argument("--disable-blink-features=AutomationControlled")
    opcoes.add_argument("--disable-popup-blocking")
    opcoes.add_argument("--disable-notifications")
    opcoes.add_argument("--log-level=3")
    opcoes.add_experimental_option("prefs", {
        "download.default_directory": pasta_temporaria,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "plugins.always_open_pdf_externally": True,
        'safebrowsing.enabled': True,
        'browser.cache.disk.enable': False,
        'network.cookie.cookieBehavior': 2,
        'credentials_enable_service': False,
        'profile.password_manager_enabled': False
    })
    return opcoes
