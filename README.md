# MPI-ENADE

<p align="center">
  <img src="https://img.shields.io/badge/C-%2300599C.svg?style=for-the-badge&logo=c&logoColor=white" alt="C">
</p>

## Descrição

Esse é um projeto desenvolvido para a disciplina de **Programação Paralela.** Esse projeto com MPI tem como objetivo utilizar os [microdados](https://www.gov.br/inep/pt-br/acesso-a-informacao/dados-abertos/microdados/enade) disponibilizados pelo inep para responder as seguintes perguntas:

1. Quantos alunos se matricularam por ações afirmativas.  
2. Qual a porcentagem de estudantes do sexo feminino matriculadas por ações afirmativas.  
3. Qual a porcentagem de estudantes do sexo feminino, com ensino técnico e provenientes de ações afirmativas.  
4. Quem deu incentivo para as estudantes do sexo feminino, de ações afirmativas, cursarem ADS.  
5. Quantos estudantes de ações afirmativas têm familiares com curso superior concluído.  
6. Quantos livros os alunos leram no ano do ENADE.  
7. Quantas horas por semana os estudantes dedicaram aos estudos.  

> **Observação:** as perguntas 2, 3 e 4 não podem ser respondidas devido ao formato dos dados.

---

## Pré‑requisitos

- Linux (ou WSL no Windows)  
- OpenMPI (`openmpi-bin` e `libopenmpi-dev`)  
- GCC (`build-essential`)

---

## Instalação
Desconsidere caso já possua um sistema linux
```bash
# (Windows) habilitar e instalar WSL
wsl install
sudo apt update && sudo apt upgrade -y

# instalar compilador e utilitários básicos
sudo apt install -y build-essential openmpi-bin libopenmpi-dev
```

## Executando o código
```bash
# Instale o MPI
sudo apt install -y openmpi-bin libopenmpi-dev

# Compile o código
mpicc -o mpi_enade mpi_enade.c
mpicc -o enade_ficticio enade_ficticio.c

# Execute o código
mpiexec -n 4 mpi_enade
mpiexec -n 4 enade_ficticio
```
