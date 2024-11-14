import os
import re
import csv
import json
import string
import logging
import pandas as pd
from requests import request
from winotify import Notification
from datetime import datetime, timedelta
from openpyxl import load_workbook
from mbk_email import Email, EmailTest, process_file_info
from digital_certificate.cert import Certificate

# Diretórios para salvar os arquivos
ROOT_DIR = r"C:\Users\miria\OneDrive\Área de Trabalho\mbk"
CSV_OUTPUT_DIR = os.path.join(ROOT_DIR, "csv")
XLSX_OUTPUT_DIR = os.path.join(ROOT_DIR, "xlsx")
LOG_DIR = os.path.join(ROOT_DIR, "logs")
PWD_DIR = os.path.join(ROOT_DIR, "pwd")

# Configuração de logs
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)

log_filename = os.path.join(LOG_DIR, f"processamento_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
logging.basicConfig(filename=log_filename, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Caminho dos arquivos CSV de clientes
CLIENTES_ATIVOS_PJ_CSV = os.path.join(CSV_OUTPUT_DIR, "Clientes ativos - PJ.csv")
CLIENTES_ATIVOS_PF_CSV = os.path.join(CSV_OUTPUT_DIR, "Clientes ativos - PF.csv")
CLIENTES_INATIVOS_PF_PJ_CSV = os.path.join(CSV_OUTPUT_DIR, "Clientes inativos - PJ e PF.csv")

# Carregar planilhas existentes (?????)
cnpj_wb = load_workbook(os.path.join(ROOT_DIR, "Clientes ativos - PJ.xlsx"))
cnpj_ws = cnpj_wb.active

cpf_wb = load_workbook(os.path.join(ROOT_DIR, "Clientes ativos - PF.xlsx"))
cpf_ws = cpf_wb.active

inativos_wb = load_workbook(os.path.join(ROOT_DIR, "Clientes inativos - PJ e PF.xlsx"))
inativos_ws = inativos_wb.active

# Criação do dicionário de contagem de dias
today = datetime.today()
counter = {str(i): today + timedelta(days=i) for i in range(1, 16)}

PWD = open(os.path.join(PWD_DIR, "pwd.txt"), "r").read()
email = Email("contato@mbkcontabilidade.com", PWD)

def get_client_data(cnpj):
    """Pega dados do cliente por meio da API minhareceita"""
    local_url = f"http://192.168.0.105:8000/{cnpj}"
    ext_url = f"https://minhareceita.org/{cnpj}"
    try:
        response = request("GET", local_url, timeout=2)
    except Exception:
        response = request("GET", ext_url)

    response = json.loads(response.text)
    logging.info(f"Dados obtidos da API para o CNPJ {cnpj}: {response}")

    formatted_cnpj = f"{cnpj[:2]}.{cnpj[2:5]}.{cnpj[5:8]}/{cnpj[8:12]}-{cnpj[12:14]}"

    # Verificar se as chaves existem antes de acessá-las
    opcao_simples = response.get("opcao_pelo_simples", False)
    opcao_mei = response.get("opcao_pelo_mei", False)

    if opcao_simples and opcao_mei:
        frame = "MEI"
    elif opcao_simples:
        frame = "SIMPLES"
    elif opcao_mei:
        frame = "MEI"
    else:
        frame = " - "

    # Checar se a chave 'qsa' existe e tem conteúdo
    if "qsa" in response and response["qsa"]:
        responsable = response["qsa"][0]["nome_socio"]
    else:
        responsable = ""
    
    partners = [socio["nome_socio"] for socio in response.get("qsa", [])]
    partners = ", ".join(partners)

    # Verificar se 'data_inicio_atividade' está presente
    if "data_inicio_atividade" in response:
        year, month, day = response["data_inicio_atividade"].split("-")
        start_at = f"{day}/{month}/{year}"
    else:
        start_at = "Data não disponível"  # Valor padrão caso não esteja presente

    data = (
        [response["razao_social"], frame]
        + [None] * 2
        + [expires_at, partners, responsable]
        + [None]
        + [cnpj, formatted_cnpj]
        + [None] * 3
        + [response.get("ddd_telefone_1", ""), response.get("email", "")]
        + [None] * 13
        + [
            response.get("descricao_identificador_matriz_filial", ""),
            start_at,
            response.get("natureza_juridica", ""),
            response.get("uf", ""),
        ]
    )
    logging.info(f"Dados formatados para o CNPJ {cnpj}: {data}")
    return data

def process_ex_client(client):
    """Registrar ex clientes"""
    old_client = False
    with open(CLIENTES_INATIVOS_PF_PJ_CSV, encoding="ISO-8859-1") as f:
        reader = csv.reader(f)
        cnpj = client[8]
        CLIENTES_INATIVOS_PF_PJ_CSV = list(reader)

        # Trocando strings vazias por None, para coincidir com o argumento dado
        for ex_client in CLIENTES_INATIVOS_PF_PJ_CSV:
            for element in ex_client:
                if element == "":
                    ex_client[ex_client.index(element)] = None

        for ex_client in CLIENTES_INATIVOS_PF_PJ_CSV:
            if cnpj == ex_client[8] or client[10] == ex_client[10]:
                logging.info(f"{ex_client[0]} já foi nosso cliente")
                old_client = ex_client
                for element in ex_client:
                    ex_client[ex_client.index(element)] = client[ex_client.index(element)]
                break

        if not old_client:
            logging.info(f"O cliente {client[0]} foi movido para a lista de inativos")
            CLIENTES_INATIVOS_PF_PJ_CSV.append(client)
        with open(CLIENTES_INATIVOS_PF_PJ_CSV, "w", encoding="ISO-8859-1", newline="") as f:
            writer = csv.writer(f)
            for ex_client in CLIENTES_INATIVOS_PF_PJ_CSV:
                writer.writerow(ex_client)

def check_ex_client(client):
    """Verificar se o cliente supostamente novo já não foi um cliente antes"""
    old_client = False
    with open(CLIENTES_INATIVOS_PF_PJ_CSV, encoding="ISO-8859-1") as f:
        reader = csv.reader(f)

        CLIENTES_INATIVOS_PF_PJ_CSV = list(reader)
        # Trocando strings vazias por None, para coincidir com o argumento dado
        for ex_client in CLIENTES_INATIVOS_PF_PJ_CSV:
            for element in ex_client:
                if element == "":
                    ex_client[ex_client.index(element)] = None

        for ex_client in CLIENTES_INATIVOS_PF_PJ_CSV:
            if client == ex_client[8] or client == ex_client[10]:
                logging.info(f"{ex_client[0]} já foi nosso cliente")
                old_client = ex_client
                break

    if old_client:
        return old_client
    else:
        return False

def email_already_sent_today(log_filename, client_email):
    """Verifica se um email já foi enviado hoje para o cliente"""
    today_date = datetime.now().strftime('%Y-%m-%d')
    client_email = str(client_email)  # Garantir que client_email seja uma string
    logging.info(f"Verificando se e-mail já foi enviado hoje para {client_email}")

    if os.path.exists(log_filename):
        with open(log_filename, 'r') as log_file:
            for line in log_file:
                if today_date in line and client_email in line:
                    logging.info(f"E-mail já foi enviado hoje para {client_email}")
                    return True
    logging.info(f"Nenhum e-mail enviado hoje para {client_email}")
    return False


# abrir os certificados digitais:
client_info_pj = {}
client_info_pf = {}
processed = set()
pfxs_directory = os.path.join(ROOT_DIR, "Certificados Digitais")
for subdir, dirs, files in os.walk(pfxs_directory):
    for file in files:
        if (
            (file.endswith(".p12")
            and "VENCIDOS" not in subdir)
            or file.endswith("pfx")
            and "VENCIDOS" not in subdir
        ):
            pfx_file = os.path.join(subdir, file)
            password_match = re.findall(r"\[(.*?)\]", file)
            if password_match:
                password = password_match[0]
            else:
                raise ValueError(f"A senha para o arquivo '{file}' não foi encontrada no nome do arquivo.")

            certificate = Certificate(pfx_file, password.encode())
            certificate.read_pfx_file()

            expires_at = certificate.not_valid_after()
            expires_at = expires_at.strftime("%d/%m/%Y")
            commom_name = certificate.common_name()

            name, cnpj = commom_name.split(":")
            logging.info(f"Certificado encontrado: {name}, CNPJ/CPF: {cnpj}, expira em {expires_at}")

            if len(cnpj) > 11:
                if cnpj not in processed:
                    processed.add(cnpj)
                    if cnpj == "35419873000118":
                        continue
                    else:
                        api_data = get_client_data(cnpj)
                        client_info_pj[cnpj] = api_data
            else:
                if cnpj not in processed:
                    processed.add(cnpj)
                    formatted_cpf = f"{cnpj[:3]}.{cnpj[3:6]}.{cnpj[6:9]}-{cnpj[9:12]}"
                    client_info_pf[cnpj] = (
                        [name]
                        + [None] * 3
                        + [expires_at]
                        + [None] * 5
                        + [cnpj, formatted_cpf]
                        + [None] * 22
                    )

            for day_str, day in counter.items():
                if expires_at == day.strftime("%d/%m/%Y"):
                    logging.info(f"Certificado {name} vai expirar em {day_str} dias")
                    
                    phone_number = client_info_pj[cnpj][11]  # Assumindo que o número de telefone está no índice 11
                    # Remova quaisquer caracteres que não sejam dígitos
                    if phone_number:
                        phone_number = re.sub(r'\D', '', phone_number)
                        # Construa a URL com o número de telefone formatado
                        wa_link = f"https://wa.me/{phone_number}"
                    else:
                        wa_link = "https://wa.me/"  # Link vazio se não houver número de telefone
                        logging.warning(f"Número de telefone não disponível para o cliente {cnpj}")

                    # Cria a notificação
                    try:
                        toast = Notification(
                            app_id="MBK Contabilidade",
                            title=f"Certificado {name} vai expirar daqui a {day_str} dias!",
                            msg=f"O certificado expira no dia {expires_at}. Bora mandar mensagem avisando!",
                            duration="long",
                            icon=r"C:\Users\lucas\Desktop\MBK-app\mbkapp\static\main\logo_white.png",
                        )
                        toast.add_actions(label="Mandar mensagem", launch=wa_link)
                        toast.show()

                    except Exception as e:
                        logging.error(f"Falha ao exibir notificação: {e}")

                    client_email = process_file_info(client_info_pj[cnpj][0])
                    if not email_already_sent_today(log_filename, client_email):
                        email.send(
                            client_email,
                            f"O seu Certificado Digital vai expirar daqui a {day_str} dias!",
                            f"O certificado expira no dia {expires_at}. Vamos renovar!",
                        )
                        logging.info(f"E-mail enviado para {client_email} sobre expiração do certificado")
                    else:
                        logging.info(f"E-mail já enviado para {client_email} hoje. Não enviado novamente.")


# Criar listas de clientes do certificado e da tabela
pj_clients = sorted(list(client_info_pj.keys()))
pj_clients.append("35419873000118")  # É preciso que o CNPJ da MBK fique por último
client_info_pj["35419873000118"] = get_client_data("35419873000118")
pj_table_clients = sorted([str(cell.value) for cell in cnpj_ws["I"] if cell.value != None and cell.value != "CNPJ S/ PONTUAÇÃO"])

pf_clients = sorted(list(client_info_pf.keys()))
pf_table_clients = sorted([int(cell.value) for cell in cpf_ws["K"] if cell.value != None and cell.value != "CPF S/ PONTUAÇÃO"])

logging.info("Clientes PJ na tabela: " + ", ".join(pj_table_clients))
logging.info("Clientes PJ dos certificados: " + ", ".join(pj_clients))
logging.info("Clientes PF na tabela: " + ", ".join(map(str, pf_table_clients)))
logging.info("Clientes PF dos certificados: " + ", ".join(pf_clients))

# Associar todos os clientes às suas linhas
x = 0   
for cell in cnpj_ws["I"]:
    x += 1
    for cnpj in list(client_info_pj.keys()):
        if cnpj == cell.value:
            client_info_pj[cnpj] = [i.value for i in cnpj_ws[x]]
            for i in cnpj_ws[x]:
                i.value = None

x = 0
for cell in cpf_ws["K"]:
    x += 1
    for cpf in list(client_info_pf.keys()):
        if cpf == cell.value:
            client_info_pf[cpf] = [i.value for i in cpf_ws[x]]
            for i in cpf_ws[x]:
                i.value = None

collumns = list(string.ascii_uppercase)
collumns.extend(["AA", "AB", "AC", "AD", "AE", "AF"])

# Verificar diferença entre as duas listas criadas
if pj_clients != pj_table_clients:
    if len(pj_clients) > len(pj_table_clients):
        diff = list(set(pj_clients).difference(pj_table_clients))
        # verificar se cliente já foi cliente antes
        for client in diff:
            is_old_client = check_ex_client(client)
            if is_old_client:
                client_info_pj[client] = is_old_client
            else:
                continue
    elif len(pj_clients) < len(pj_table_clients):
        diff = list(set(pj_table_clients).difference(pj_clients))
        for client in diff:
            x = 0
            for cell in cnpj_ws["I"]:
                x += 1
                if client == cell.value:
                    ex_client = [i.value for i in cnpj_ws[x]]
                    process_ex_client(ex_client)
                    for i in cnpj_ws[x]:
                        i.value = None
                else:
                    continue
else:
    logging.info("Nenhuma diferença encontrada entre os clientes PJ dos certificados e da tabela")

if pf_clients != pf_table_clients:
    if len(pf_clients) > len(pf_table_clients):
        diff = list(set(pf_clients).difference(pf_table_clients))
        # verificar se cliente já foi cliente antes
        for client in diff:
            is_old_client = check_ex_client(client)
            if is_old_client:
                client_info_pf[client] = is_old_client
            else:
                continue
    elif len(pf_clients) < len(pf_table_clients):
        diff = list(set(pf_table_clients).difference(pf_clients))
        for client in diff:
            x = 0
            for cell in cpf_ws["K"]:
                x += 1
                if client == cell.value:
                    ex_client = [i.value for i in cpf_ws[x]]
                    process_ex_client(ex_client)
                    for i in cpf_ws[x]:
                        i.value = None
                else:
                    continue
else:
    logging.info("Nenhuma diferença encontrada entre os clientes PF dos certificados e da tabela")

i = 1
for client in list(client_info_pj.keys()):
    i += 1
    for collumn in collumns:
        cnpj_ws[f"{collumn}{i}"] = client_info_pj[client][collumns.index(collumn)]

i = 1
for client in list(client_info_pf.keys()):
    i += 1
    for collumn in collumns:
        cpf_ws[f"{collumn}{i}"] = client_info_pf[client][collumns.index(collumn)]

# Salvar os arquivos XLSX no diretório especificado
cpf_wb.save(os.path.join(XLSX_OUTPUT_DIR, "Clientes ativos - PF.xlsx"))
cnpj_wb.save(os.path.join(XLSX_OUTPUT_DIR, "Clientes ativos - PJ.xlsx"))
inativos_wb.save(os.path.join(XLSX_OUTPUT_DIR, "Clientes inativos - PJ e PF.xlsx"))

logging.info("Arquivos XLSX salvos com sucesso")

# Salvar os arquivos CSV no novo diretório
with open(CLIENTES_ATIVOS_PJ_CSV, "w", encoding="ISO-8859-1", newline="") as f:
    writer = csv.writer(f)
    for client in client_info_pj.keys():
        writer.writerow(client_info_pj[client])

with open(CLIENTES_ATIVOS_PF_CSV, "w", encoding="ISO-8859-1", newline="") as f:
    writer = csv.writer(f)
    for client in client_info_pf.keys():
        writer.writerow(client_info_pf[client])

with open(CLIENTES_INATIVOS_PF_PJ_CSV, "w", encoding="ISO-8859-1", newline="") as f:
    writer = csv.writer(f)
    for row in inativos_ws.iter_rows(values_only=True):
        writer.writerow(row)

logging.info("Arquivos CSV salvos com sucesso")
