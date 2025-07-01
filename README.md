# MPI-ENADE

<p align="center">
  <img src="https://img.shields.io/badge/C-%2300599C.svg?style=for-the-badge&logo=c&logoColor=white" alt="C">
</p>

## Descrição

Esse é um projeto desenvolvido para a disciplina de **Programação Paralela.** Esse projeto com MPI tem como objetivo utilizar os [microdados](https://www.gov.br/inep/pt-br/acesso-a-informacao/dados-abertos/microdados/enade) disponibilizados pelo inep para responder as seguintes perguntas:

1. Quantos alunos se matricularam no curso? 
2. Qual é a porcentagem de estudante do sexo Feminino que se formaram?
3. Qual é a porcentagem de estudantes que cursaram o ensino técnico no ensino médio?
4. Qual é o percentual de alunos provenientes de ações afirmativas?
5. Dos estudantes, responda quem deu incentivo para este estudante cursar o ADS.
6. Quantos estudantes apresentaram familiares com o curso superior concluído?
7. Quantos livros os alunos leram no ano do ENADE?
8. Quantas horas na semana os estudantes se dedicaram aos estudos?

> **Observação:** os código "totalAlunos.c" não depende de MPI e foi utilizado para obter o total de alunos matriculados em ads para comparações com o código principal.

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
sudo apt install -y openmpi-bin openmpi-common libopenmpi-dev

# Compile o código
mpicc -o mpi_enade mpi_enade.c

# Execute o código
mpiexec -n 4 mpi_enade
```
