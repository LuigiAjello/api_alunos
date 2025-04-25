from fastapi import FastAPI
from sqlalchemy import create_engine, text 
import pymysql
import os
import pandas as pd

from db_connection import engine

def get_id_by_nome(nome_aluno):
    """
    Obtém o ID de um aluno a partir do seu nome.

    Args:
        nome_aluno (str): Nome do aluno a ser pesquisado

    Returns:
        int: ID do aluno encontrado ou None se não existir
    """
    sql = text("SELECT id FROM tb_alunos WHERE nome_aluno = :nome_aluno")
    with engine.connect() as conn:
        result = conn.execute(sql, {'nome_aluno': nome_aluno}).fetchone()
    return result[0] if result else None


def get_id_by_nome_disciplina(nome_disciplina):
    """
    Obtém o ID de uma disciplina a partir do seu nome.

    Args:
        nome_disciplina (str): Nome da disciplina a ser pesquisada

    Returns:
        int: ID da disciplina encontrada ou None se não existir
    """
    sql = text("SELECT id FROM tb_disciplinas WHERE nome_disciplina = :nome_disciplina")
    with engine.connect() as conn:
        result = conn.execute(sql, {'nome_disciplina': nome_disciplina}).fetchone()
    return result[0] if result else None


def get_IdNota_by_nome_disciplina_(nome_aluno: str, nome_disciplina: str):
    """
    Obtém o ID do registro de nota a partir do nome do aluno e da disciplina.

    Args:
        nome_aluno (str): Nome do aluno
        nome_disciplina (str): Nome da disciplina

    Returns:
        int: ID da nota ou None se não encontrada
    """
    aluno_id = get_id_by_nome(nome_aluno)
    disciplina_id = get_id_by_nome_disciplina(nome_disciplina)

    if aluno_id is None or disciplina_id is None:
        return None

    sql = text("""
        SELECT id FROM tb_notas
        WHERE aluno_id = :aluno_id AND disciplina_id = :disciplina_id
    """)
    
    with engine.connect() as conn:
        result = conn.execute(sql, {'aluno_id': aluno_id, 'disciplina_id': disciplina_id}).fetchone()
    
    return result[0] if result else None



