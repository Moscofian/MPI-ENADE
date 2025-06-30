// Arquivo: analisador_enade_pt.c
// Descrição: Versão final do analisador de dados do ENADE 2021 em português.
//            Processa cada arquivo de forma independente e apresenta os resultados
//            com totais por pergunta e porcentagens de resposta.
//            Projetado para ser executado com EXATAMENTE 4 processos.

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <mpi.h>
#include <ctype.h>

// =================================================================================
// --- SEÇÃO DE CONFIGURAÇÃO ---
// =================================================================================

const char* ARQUIVOS_QUESTOES[] = {
    "microdados2021_arq21.txt", // 0: QE_I15 (Ação Afirmativa)
    "microdados2021_arq5.txt",  // 1: TP_SEXO (Impossível de usar para cruzar dados)
    "microdados2021_arq24.txt", // 2: QE_I18 (Impossível de usar)
    "microdados2021_arq25.txt", // 3: QE_I19 (Impossível de usar)
    "microdados2021_arq27.txt", // 4: QE_I21 (Impossível de usar)
    "microdados2021_arq28.txt", // 5: QE_I22 (Livros lidos)
    "microdados2021_arq29.txt"  // 6: QE_I23 (Horas de estudo)
};
// Enum para facilitar a leitura do código, associando um nome ao índice do arquivo
enum IndiceQuestao { ACAO_AFIRMATIVA = 0, SEXO = 1, TIPO_EM = 2, INCENTIVO = 3, ESCOLARIDADE_FAM = 4, LIVROS = 5, HORAS_ESTUDO = 6 };

const int NUM_ARQUIVOS_QUESTOES = 7;
const char* ARQUIVO_MAPA_CURSOS = "microdados2021_arq1.txt";
const long CODIGO_GRUPO_TADS = 72;

#define TAM_MAX_LINHA 512
#define TAM_CODIGO_CURSO 10

// =================================================================================
// --- ESTRUTURAS DE DADOS E FUNÇÕES AUXILIARES ---
// =================================================================================

// Estrutura para armazenar os resultados da análise
typedef struct {
    long alunos_aa_sim;
    long alunos_aa_nao;
    // Livros
    long livros_nenhum; long livros_1_a_2; long livros_3_a_5; long livros_6_a_8; long livros_mais_de_8;
    // Horas de Estudo
    long estudo_nenhuma; long estudo_1_a_3; long estudo_4_a_7; long estudo_8_a_12; long estudo_mais_de_12;
} ResultadosAnalise;

// Função para limpar aspas e espaços em branco do início e fim de uma string
void limpar_aspas_e_espacos(char *str) {
    if (str == NULL || *str == '\0') return;
    char *inicio = str;
    while (isspace((unsigned char)*inicio) || *inicio == '"') inicio++;
    char *fim = str + strlen(str) - 1;
    while (fim > inicio && (isspace((unsigned char)*fim) || *fim == '"')) fim--;
    *(fim + 1) = '\0';
    if (inicio != str) memmove(str, inicio, strlen(inicio) + 1);
}

// Verifica se um código de curso está na lista de cursos de TADS
int eh_curso_tads(const char* codigo_curso, char (*lista_tads)[TAM_CODIGO_CURSO], int contagem) {
    for (int i = 0; i < contagem; i++) {
        if (strcmp(codigo_curso, lista_tads[i]) == 0) return 1;
    }
    return 0;
}

// Processa um único arquivo de perguntas do início ao fim
void processar_arquivo_unico(const char* nome_arquivo, int indice_arquivo, char (*lista_codigos_tads)[TAM_CODIGO_CURSO], int contagem_tads, ResultadosAnalise* resultados) {
    FILE* fp = fopen(nome_arquivo, "r");
    if (!fp) {
        fprintf(stderr, "Aviso: Não foi possível abrir o arquivo %s. Ignorando.\n", nome_arquivo);
        return;
    }
    char linha[TAM_MAX_LINHA];
    fgets(linha, sizeof(linha), fp); // Pula o cabeçalho
    while (fgets(linha, sizeof(linha), fp)) {
        char* copia_linha = strdup(linha);
        char* saveptr; strtok_r(copia_linha, ";", &saveptr);
        char* str_co_curso = strtok_r(NULL, ";", &saveptr);
        char* resposta = strtok_r(NULL, "\r\n", &saveptr);
        if (!str_co_curso || !resposta) { free(copia_linha); continue; }
        
        limpar_aspas_e_espacos(str_co_curso);
        limpar_aspas_e_espacos(resposta);
        
        if (eh_curso_tads(str_co_curso, lista_codigos_tads, contagem_tads)) {
            switch (indice_arquivo) {
                case ACAO_AFIRMATIVA:
                    if (strcmp(resposta, "A") == 0) {
                        resultados->alunos_aa_nao++;
                    } else if (strlen(resposta) > 0 && strcmp(resposta, ".") != 0) {
                        resultados->alunos_aa_sim++;
                    }
                    break;
                case LIVROS:
                    if (strcmp(resposta, "A") == 0) resultados->livros_nenhum++;
                    else if (strcmp(resposta, "B") == 0) resultados->livros_1_a_2++;
                    else if (strcmp(resposta, "C") == 0) resultados->livros_3_a_5++;
                    else if (strcmp(resposta, "D") == 0) resultados->livros_6_a_8++;
                    else if (strcmp(resposta, "E") == 0) resultados->livros_mais_de_8++;
                    break;
                case HORAS_ESTUDO:
                    if (strcmp(resposta, "A") == 0) resultados->estudo_nenhuma++;
                    else if (strcmp(resposta, "B") == 0) resultados->estudo_1_a_3++;
                    else if (strcmp(resposta, "C") == 0) resultados->estudo_4_a_7++;
                    else if (strcmp(resposta, "D") == 0) resultados->estudo_8_a_12++;
                    else if (strcmp(resposta, "E") == 0) resultados->estudo_mais_de_12++;
                    break;
            }
        }
        free(copia_linha);
    }
    fclose(fp);
}

// =================================================================================
// --- FUNÇÃO PRINCIPAL ---
// =================================================================================
int main(int argc, char** argv) {
    MPI_Init(&argc, &argv);
    int meu_rank, num_processos;
    MPI_Comm_rank(MPI_COMM_WORLD, &meu_rank);
    MPI_Comm_size(MPI_COMM_WORLD, &num_processos);
    
    // VERIFICAÇÃO: Garante que o programa seja executado com exatamente 4 processos.
    if (num_processos != 4) {
        if (meu_rank == 0) { // Apenas o processo mestre imprime a mensagem de erro.
            fprintf(stderr, "Erro: Este programa foi projetado para ser executado com exatamente 4 processos.\n");
            fprintf(stderr, "Por favor, execute com: mpiexec -n 4 %s\n", argv[0]);
        }
        MPI_Finalize();
        return 1; // Encerra a execução para todos os processos
    }
    
    // ETAPAS 1 e 2: Mapeamento e Broadcast dos cursos
    char (*lista_codigos_tads)[TAM_CODIGO_CURSO] = NULL;
    int contagem_cursos_tads = 0;
    if (meu_rank == 0) {
        printf("Mestre (Rank 0): Mapeando cursos de TADS (Grupo %ld)...\n", CODIGO_GRUPO_TADS);
        FILE* fp = fopen(ARQUIVO_MAPA_CURSOS, "r"); if (!fp) { MPI_Abort(MPI_COMM_WORLD, 1); }
        char linha[TAM_MAX_LINHA]; fgets(linha, sizeof(linha), fp);
        while (fgets(linha, sizeof(linha), fp)) {
            char* copia_linha = strdup(linha); char* saveptr; strtok_r(copia_linha, ";", &saveptr);
            char* str_co_curso = strtok_r(NULL, ";", &saveptr);
            strtok_r(NULL, ";", &saveptr); strtok_r(NULL, ";", &saveptr); strtok_r(NULL, ";", &saveptr);
            char* str_co_grupo = strtok_r(NULL, ";", &saveptr);
            if (str_co_grupo && str_co_curso && atol(str_co_grupo) == CODIGO_GRUPO_TADS) {
                limpar_aspas_e_espacos(str_co_curso); int encontrado = 0;
                for (int i = 0; i < contagem_cursos_tads; i++) { if (strcmp(lista_codigos_tads[i], str_co_curso) == 0) { encontrado = 1; break; } }
                if (!encontrado) {
                    contagem_cursos_tads++; lista_codigos_tads = realloc(lista_codigos_tads, contagem_cursos_tads * sizeof(*lista_codigos_tads));
                    strncpy(lista_codigos_tads[contagem_cursos_tads - 1], str_co_curso, TAM_CODIGO_CURSO - 1);
                    lista_codigos_tads[contagem_cursos_tads - 1][TAM_CODIGO_CURSO - 1] = '\0';
                }
            } free(copia_linha);
        } fclose(fp);
        printf("Mestre (Rank 0): Mapeamento concluído. %d cursos de TADS únicos encontrados.\n", contagem_cursos_tads);
    }
    MPI_Bcast(&contagem_cursos_tads, 1, MPI_INT, 0, MPI_COMM_WORLD);
    if (meu_rank != 0) { lista_codigos_tads = malloc(contagem_cursos_tads * sizeof(*lista_codigos_tads)); }
    MPI_Bcast(lista_codigos_tads, contagem_cursos_tads * TAM_CODIGO_CURSO, MPI_CHAR, 0, MPI_COMM_WORLD);

    // ETAPAS 3 e 4: Processamento e Agregação
    ResultadosAnalise resultados_locais = {0};
    for (int i = meu_rank; i < NUM_ARQUIVOS_QUESTOES; i += num_processos) {
        processar_arquivo_unico(ARQUIVOS_QUESTOES[i], i, lista_codigos_tads, contagem_cursos_tads, &resultados_locais);
    }
    free(lista_codigos_tads);
    ResultadosAnalise resultados_globais = {0};
    MPI_Reduce(&resultados_locais, &resultados_globais, sizeof(ResultadosAnalise)/sizeof(long), MPI_LONG, MPI_SUM, 0, MPI_COMM_WORLD);

    // --- ETAPA 5: APRESENTAÇÃO DOS RESULTADOS (RANK 0) ---
    if (meu_rank == 0) {
        printf("\n--- RESULTADOS DA ANÁLISE ENADE 2021 (CURSO: TADS) ---\n\n");
        printf("AVISO IMPORTANTE: Devido à estrutura dos dados, apenas as perguntas\n");
        printf("que podem ser respondidas de forma independente são mostradas abaixo.\n\n");
        
        long total_respostas_aa = resultados_globais.alunos_aa_sim + resultados_globais.alunos_aa_nao;
        printf("1. Análise sobre ingresso por Ações Afirmativas (A.A.)\n");
        printf("   Total de respostas válidas para esta pergunta: %ld\n", total_respostas_aa);
        printf("   - Sim, ingressou por A.A.: ............ %ld (%.2f%%)\n", resultados_globais.alunos_aa_sim, (total_respostas_aa > 0) ? (double)resultados_globais.alunos_aa_sim / total_respostas_aa * 100.0 : 0.0);
        printf("   - Não, ingressou por ampla concorrência: %ld (%.2f%%)\n\n", resultados_globais.alunos_aa_nao, (total_respostas_aa > 0) ? (double)resultados_globais.alunos_aa_nao / total_respostas_aa * 100.0 : 0.0);
        
        long total_respostas_livros = resultados_globais.livros_nenhum + resultados_globais.livros_1_a_2 + resultados_globais.livros_3_a_5 + resultados_globais.livros_6_a_8 + resultados_globais.livros_mais_de_8;
        printf("2. Quantos livros os alunos de TADS leram neste ano?\n");
        printf("   Total de respostas válidas para esta pergunta: %ld\n", total_respostas_livros);
        printf("   - Nenhum: ............................... %ld (%.2f%%)\n", resultados_globais.livros_nenhum, (total_respostas_livros > 0) ? (double)resultados_globais.livros_nenhum / total_respostas_livros * 100.0 : 0.0);
        printf("   - Um ou dois: ........................... %ld (%.2f%%)\n", resultados_globais.livros_1_a_2, (total_respostas_livros > 0) ? (double)resultados_globais.livros_1_a_2 / total_respostas_livros * 100.0 : 0.0);
        printf("   - Três a cinco: ......................... %ld (%.2f%%)\n", resultados_globais.livros_3_a_5, (total_respostas_livros > 0) ? (double)resultados_globais.livros_3_a_5 / total_respostas_livros * 100.0 : 0.0);
        printf("   - Seis a oito: .......................... %ld (%.2f%%)\n", resultados_globais.livros_6_a_8, (total_respostas_livros > 0) ? (double)resultados_globais.livros_6_a_8 / total_respostas_livros * 100.0 : 0.0);
        printf("   - Mais de oito: ......................... %ld (%.2f%%)\n\n", resultados_globais.livros_mais_de_8, (total_respostas_livros > 0) ? (double)resultados_globais.livros_mais_de_8 / total_respostas_livros * 100.0 : 0.0);
        
        long total_respostas_estudo = resultados_globais.estudo_nenhuma + resultados_globais.estudo_1_a_3 + resultados_globais.estudo_4_a_7 + resultados_globais.estudo_8_a_12 + resultados_globais.estudo_mais_de_12;
        printf("3. Quantas horas semanais os estudantes de TADS se dedicaram aos estudos?\n");
        printf("   Total de respostas válidas para esta pergunta: %ld\n", total_respostas_estudo);
        printf("   - Nenhuma: .............................. %ld (%.2f%%)\n", resultados_globais.estudo_nenhuma, (total_respostas_estudo > 0) ? (double)resultados_globais.estudo_nenhuma / total_respostas_estudo * 100.0 : 0.0);
        printf("   - De uma a três: ........................ %ld (%.2f%%)\n", resultados_globais.estudo_1_a_3, (total_respostas_estudo > 0) ? (double)resultados_globais.estudo_1_a_3 / total_respostas_estudo * 100.0 : 0.0);
        printf("   - De quatro a sete: ..................... %ld (%.2f%%)\n", resultados_globais.estudo_4_a_7, (total_respostas_estudo > 0) ? (double)resultados_globais.estudo_4_a_7 / total_respostas_estudo * 100.0 : 0.0);
        printf("   - De oito a doze: ....................... %ld (%.2f%%)\n", resultados_globais.estudo_8_a_12, (total_respostas_estudo > 0) ? (double)resultados_globais.estudo_8_a_12 / total_respostas_estudo * 100.0 : 0.0);
        printf("   - Mais de doze: ......................... %ld (%.2f%%)\n\n", resultados_globais.estudo_mais_de_12, (total_respostas_estudo > 0) ? (double)resultados_globais.estudo_mais_de_12 / total_respostas_estudo * 100.0 : 0.0);
        
        printf("-----------------------------------------------------------\n");
    }

    MPI_Finalize();
    return 0;
}
