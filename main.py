import pyodbc
from Funcoes import *
import mysql.connector
from sqlalchemy import create_engine
import urllib.parse

# Lista de distribuidoras
distribuidoras = ["EQUATORIAL GO"]


# Credenciais para conexão com o sql heidi
local = 'gestaodb2.americaenergia.com.br'
usuario = 'america_gestao'
senha = 'HTHhdt6352s!23'
bancodedados = 'america_gestao'

#Extraindo ano atual e mes atual
mes_atual_sql = str(datetime.now().month).zfill(2) 
ano_atual_sql = str(datetime.now().year)

# Conexão com o MySQL
conexao = mysql.connector.connect(host=local,user=usuario,password=senha,database=bancodedados)
encoded_password = urllib.parse.quote_plus(senha)
connection_url = (f"mysql+mysqlconnector://{usuario}:{encoded_password}"f"@{local}:{3306}/{bancodedados}")
engine = create_engine(connection_url)
db_config = DatabaseConfig(usuario, senha)
engine = db_config.connect()
TBL_CADASTRAL = 'tb_clientes_gestao_faturas'
TBL_FATURAS = 'tb_dfat_gestao_faturas_energia_novo'
COLUNAS_CADASTRAL = ['GRUPO', 'DISTRIBUIDORA', 'INSTALACAO_MATRICULA', 'CLASSE', 'LOGIN', 'SENHA', 'PASSO_ADICIONAL']
COLUNAS_FATURAS = ['COD_INSTALACAO','REFERENCIA', 'DATA_VENCIMENTO']
df_logins = read_table(TBL_CADASTRAL, engine, COLUNAS_CADASTRAL, f"DISTRIBUIDORA = '{distribuidoras[0]}' AND STATUS_UNIDADE <> 'Inativa'")
df_faturas = read_table(TBL_FATURAS, engine, COLUNAS_FATURAS, f"DISTRIBUIDORA = '{distribuidoras[0]}' AND REFERENCIA = '{ano_atual_sql}-{mes_atual_sql}-01'")


# Conexão com o acces para inserir dados de status de pagamento e coleta
conn_string = (r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};'r'DBQ=G:\QUALIDADE\11. ARQUIVOS COLABORADORES\Abdul\Logins 2.accdb;')
    

# Conexão com o banco de dados
conn = pyodbc.connect(conn_string)


# Para cada distribuidora, execute o código
for dist in distribuidoras:
    

    # Filtrando os logins conforme inserido pelo usuário
    logins_filtrados = filtrar_clientes(df_logins, dist)
 

    # Criar uma lista vazia para armazenar os logins
    lista_logins = []
 

    # Iterar sobre os logins filtrados e adicionar os valores das colunas à lista
    for _, login in logins_filtrados.iterrows():
        lista_logins.append({
            'INSTALACAO_MATRICULA': login['INSTALACAO_MATRICULA'],
            'DISTRIBUIDORA': login['DISTRIBUIDORA'],
            'LOGIN': login['LOGIN'],
            'SENHA': login['SENHA'],
            'CLASSE': login['CLASSE'],
            'GRUPO': login['GRUPO'],
            'PASSO_ADICIONAL': login['PASSO_ADICIONAL']
        })
 

    # Importar a função Download dinamicamente
    Download = getattr(__import__(f"{dist}", fromlist=["main"]), "main")

    # Começa a realizar a coleta a partir da distribuidora pertencente a lista
    try:
        Download(lista_logins, conn,df_faturas)

    except Exception as e:
        print(f"Erro ao rodar o código da distribuidora {dist}: {e}")
 

# Fechar a conexão com o banco de dados após o término do loop
conn.close()