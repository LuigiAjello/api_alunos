from fastapi import FastAPI
from sqlalchemy import create_engine, text 
import pymysql
import os
import pandas as pd

from db_connection import engine
from functions import get_id_by_nome, get_id_by_nome_disciplina,get_IdNota_by_nome_disciplina_

app = FastAPI()

@app.get("/Alunos/")
def pegar_alunos():
    with engine.begin() as conn: 
        result = conn.execute(text("SELECT * FROM tb_alunos"))
        alunos = [row._asdict() for row in result]
    return alunos



@app.post("/NewAluno/")

def cadastrar_alunos(aluno: dict):
    '''
           aluno = {
                    'nome_aluno': nome_aluno,
                    'email': email,
                    'cep': cep,
                    'carro_id': carro_id,
                }
    ''' 
    sql = text("""
        INSERT INTO tb_alunos (nome_aluno, email, cep, carro_id)
        VALUES (:nome_aluno, :email, :cep, :carro_id)
        """)
    with engine.begin() as conn:  # Usa transação automática
        conn.execute(sql, aluno)

    return {"message": "Aluno cadastrado com sucesso!"}




@app.put("/UpdateAluno/{nome_aluno}")

def update_alunos(nome_aluno: str, dados_aluno_atualizar:dict):
    '''
           {
                    
                    "novo_email":"alexandre@gmail.com",
                    "novo_cep": "01001-000",
                    "novo_carro_id": "1"
                }  
        ''' 
    aluno_id = get_id_by_nome(nome_aluno)
    
    if not aluno_id:
        return "Aluno não encontrado!"
    
    campos_para_atualizar = []
    params = {"ID": aluno_id}

    if  "novo_nome" in dados_aluno_atualizar  :
        campos_para_atualizar.append("nome_aluno = :novo_nome")
        params["novo_nome"] = dados_aluno_atualizar["novo_nome"]

    if "novo_email" in dados_aluno_atualizar:
        campos_para_atualizar.append("email = :novo_email")
        params["novo_email"] = dados_aluno_atualizar["novo_email"]

    if "novo_cep" in dados_aluno_atualizar:
        campos_para_atualizar.append("cep = :novo_cep")
        params["novo_cep"] =dados_aluno_atualizar["novo_cep"]

    if "novo_carro_id" in dados_aluno_atualizar:
        campos_para_atualizar.append("carro_id = :novo_carro_id")
        params["novo_carro_id"] = dados_aluno_atualizar["novo_carro_id"]

    if not campos_para_atualizar:
        return "Nenhum campo para atualizar."

    sql = text(f"""
        UPDATE tb_alunos
        SET {", ".join(campos_para_atualizar)}
        WHERE id = :ID
    """)

    with engine.begin() as conn:
        conn.execute(sql, params)

    return "Aluno atualizado com sucesso!"


@app.delete("/DeletaAluno/{nome_aluno}")

def deletar_aluno(nome_aluno: str):
    aluno_id = get_id_by_nome(nome_aluno)

    if not aluno_id:
        return {"message": "Aluno não encontrado."}

    sql = text("DELETE FROM tb_alunos WHERE id = :aluno_id")

    with engine.begin() as conn:
        conn.execute(sql, {"aluno_id": aluno_id})

    return {"message": "Aluno deletado com sucesso!"}


##############
@app.get("/Enderecos/")
def pegar_endereco():
    with engine.begin() as conn: 
        result = conn.execute(text("SELECT * FROM tb_enderecos"))
        endereco = [row._asdict() for row in result]
    return endereco

@app.post("/NewEndereco/")
def inserir_endereco(endereco:dict):
    '''
Exemplo de JSON:
{
    "cep": "70337022",
    "endereco": "sqs 304 bloco c",
    "cidade": "brasilia",
    "estado": "DF"
}
'''
    with engine.begin() as conn:
        sql = text("""
        INSERT INTO tb_enderecos (cep, endereco, cidade, estado)
        VALUES (:cep, :endereco, :cidade, :estado)
    """)
        conn.execute(sql, endereco)
    return{"message":"endereco cadastrado com sucesso"}


@app.put("/UpdateEndereco/{cep}")
def update_endereco(cep: str, dados_endereco: dict):
    '''
    Exemplo de JSON:
    {
        "novo_cep": "70337022",
        "novo_endereco": "sqs 304 bloco c",
        "nova_cidade": "brasilia",
        "novo_estado": "DF"
    }
    '''

    if not cep:
        return {"message": "CEP original não informado."}

    campos_para_atualizar = []
    params = {"cep": cep}

    if "novo_cep" in dados_endereco:
        campos_para_atualizar.append("cep = :novo_cep")
        params["novo_cep"] = dados_endereco["novo_cep"]

    if "novo_endereco" in dados_endereco:
        campos_para_atualizar.append("endereco = :novo_endereco")
        params["novo_endereco"] = dados_endereco["novo_endereco"]

    if "nova_cidade" in dados_endereco:
        campos_para_atualizar.append("cidade = :nova_cidade")
        params["nova_cidade"] = dados_endereco["nova_cidade"]

    if "novo_estado" in dados_endereco:
        campos_para_atualizar.append("estado = :novo_estado")
        params["novo_estado"] = dados_endereco["novo_estado"]

    if not campos_para_atualizar:
        return {"message": "Nenhum campo para atualizar."}

    sql = text(f"""
        UPDATE tb_enderecos
        SET {", ".join(campos_para_atualizar)}
        WHERE cep = :cep
    """)

    with engine.begin() as conn:
        result = conn.execute(sql, params)

    return {"message": "Endereço atualizado com sucesso!"}

@app.delete("/DeletaEndereco/{cep}")

def deletar_endereco(cep: str):
    

    if not cep:
        return {"message": "Endereço não encontrado."}

    sql = text("DELETE FROM tb_enderecos WHERE cep = :cep")

    with engine.begin() as conn:
        conn.execute(sql, {"cep": cep})

    return {"message": "endereco deletado com sucesso!"}

##############

@app.get("/Disciplinas/")
def pegar_Disciplina():
    with engine.begin() as conn: 
        result = conn.execute(text("SELECT * FROM tb_disciplinas"))
        disciplina = [row._asdict() for row in result]
    return disciplina


@app.post("/NewDisciplina/")
def inserir_Disciplina(disciplina: dict):
    '''
    Exemplo de JSON:
    {
        "nome_disciplina": "Cálculo a uma Variável",
        "carga": 80,
        "semestre": 1
    }
    '''
    with engine.begin() as conn:
        sql = text("""
            INSERT INTO tb_disciplinas (nome_disciplina, carga, semestre)
            VALUES (:nome_disciplina, :carga, :semestre)
        """)
        conn.execute(sql, disciplina)
    
    return {"message": "Disciplina cadastrada com sucesso!"}





@app.put("/UpdateDisciplina/{nome_disciplina}")
def update_disciplina(nome_disciplina: str, dados_disciplina: dict):
    '''
    Exemplo de JSON:
    {
        "novo_nome_disciplina": "Cálculo a uma Variável",
        "nova_carga": 80,
        "novo_semestre": 1
    }
    '''
    id = get_id_by_nome_disciplina(nome_disciplina)

    if not id:
        return {"message": "Disciplina original não encontrada."}

    campos_para_atualizar = []
    params = {"id": id}

    if "novo_nome_disciplina" in dados_disciplina:
        campos_para_atualizar.append("nome_disciplina = :novo_nome_disciplina")
        params["novo_nome_disciplina"] = dados_disciplina["novo_nome_disciplina"]

    if "nova_carga" in dados_disciplina:
        campos_para_atualizar.append("carga = :nova_carga")
        params["nova_carga"] = dados_disciplina["nova_carga"]

    if "novo_semestre" in dados_disciplina:
        campos_para_atualizar.append("semestre = :novo_semestre")
        params["novo_semestre"] = dados_disciplina["novo_semestre"]

    if not campos_para_atualizar:
        return {"message": "Nenhum campo para atualizar."}

    sql = text(f"""
        UPDATE tb_disciplinas
        SET {", ".join(campos_para_atualizar)}
        WHERE id = :id
    """)

    with engine.begin() as conn:
        conn.execute(sql, params)

    return {"message": "Disciplina atualizada com sucesso!"}

@app.delete("/DeletaDisciplina/{nome_disciplina}")

def deletar_disciplina(nome_disciplina: str):
    id = get_id_by_nome_disciplina(nome_disciplina)

    if not nome_disciplina:
        return {"message": "Disciplina não encontrado."}

    sql = text("DELETE FROM tb_disciplinas WHERE id = :id")

    with engine.begin() as conn:
        conn.execute(sql, {"id": id})

    return {"message": "disciplina deletada com sucesso!"}


##############

@app.get("/Notas/")
def pegar_Notas():
    with engine.begin() as conn: 
        result = conn.execute(text("SELECT * FROM tb_notas"))
        notas = [row._asdict() for row in result]
    return notas

@app.post("/NewNota/{nome_aluno}/{nome_disciplina}")
def inserir_nota(nome_disciplina: str, nome_aluno: str, nota_dict: dict):
    '''
    Exemplo de JSON:
    {
        "nota": 2.0
    }
    '''
    aluno_id = get_id_by_nome(nome_aluno)
    disciplina_id = get_id_by_nome_disciplina(nome_disciplina)

    if aluno_id is None or disciplina_id is None:
        return {"message": "Aluno ou disciplina não encontrado(a)."}

    params = {
        "aluno_id": aluno_id,
        "disciplina_id": disciplina_id,
        "nota": nota_dict["nota"]
    }

    with engine.begin() as conn:
        sql = text("""
            INSERT INTO tb_notas (aluno_id, disciplina_id, nota)
            VALUES (:aluno_id, :disciplina_id, :nota)
        """)
        conn.execute(sql, params)

    return {"message": "Nota cadastrada com sucesso!"}


@app.put("/UpdateNota/{nome_aluno_antigo}/{nome_disciplina_antiga}")
def atualizar_nota_completa(nome_aluno_antigo: str, nome_disciplina_antiga: str, dados: dict):
    '''
    Exemplo de JSON:
    {
        "novo_nome_aluno": "João",
        "novo_nome_disciplina": "Álgebra Linear",
        "nova_nota": 9.0
    }
    '''

    aluno_id_antigo = get_id_by_nome(nome_aluno_antigo)
    disciplina_id_antiga = get_id_by_nome_disciplina(nome_disciplina_antiga)

    if aluno_id_antigo is None or disciplina_id_antiga is None:
        return {"message": "Aluno ou disciplina antiga não encontrada."}

    campos_para_atualizar = []
    params = {
        "aluno_id_antigo": aluno_id_antigo,
        "disciplina_id_antiga": disciplina_id_antiga,
    }

    if "novo_nome_aluno" in dados:
        novo_aluno_id = get_id_by_nome(dados["novo_nome_aluno"])
        if novo_aluno_id:
            campos_para_atualizar.append("aluno_id = :novo_aluno_id")
            params["novo_aluno_id"] = novo_aluno_id
        else:
            return {"message": "Novo aluno não encontrado."}

    if "novo_nome_disciplina" in dados:
        nova_disciplina_id = get_id_by_nome_disciplina(dados["novo_nome_disciplina"])
        if nova_disciplina_id:
            campos_para_atualizar.append("disciplina_id = :nova_disciplina_id")
            params["nova_disciplina_id"] = nova_disciplina_id
        else:
            return {"message": "Nova disciplina não encontrada."}

    if "nova_nota" in dados:
        campos_para_atualizar.append("nota = :nova_nota")
        params["nova_nota"] = dados["nova_nota"]

    if not campos_para_atualizar:
        return {"message": "Nenhum campo para atualizar."}

    sql = text(f"""
        UPDATE tb_notas
        SET {", ".join(campos_para_atualizar)}
        WHERE aluno_id = :aluno_id_antigo AND disciplina_id = :disciplina_id_antiga
    """)

    with engine.begin() as conn:
        conn.execute(sql, params)

    return {"message": "Nota (ou aluno/disciplina) atualizada com sucesso!"}



@app.delete("/DeletaNota/{nome_aluno_antigo}/{nome_disciplina_antiga}")
def deletar_nota(nome_aluno_antigo: str, nome_disciplina_antiga: str):
    """
    Deleta uma nota com base no nome do aluno e da disciplina.
    """
    nota_id = get_IdNota_by_nome_disciplina_(nome_aluno_antigo, nome_disciplina_antiga)

    if nota_id is None:
        return {"message": "Nota não encontrada para o aluno e disciplina informados."}

    sql = text("DELETE FROM tb_notas WHERE id = :id")

    with engine.begin() as conn:
        conn.execute(sql, {"id": nota_id})

    return {"message": "Nota deletada com sucesso!"}




@app.get("/")
def home():

    return{"message":"minha primeira api"}
