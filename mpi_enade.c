// Arquivo: analisador_enade_final_comentado.c
// Este programa realiza a análise completa dos microdados do ENADE 2021 para TADS.
// Ele foi projetado para ser robusto e funciona em um modelo "Gerente/Trabalhador" com MPI.
// IMPORTANTE: Compile e execute este programa de qualquer diretório, pois os caminhos dos arquivos são absolutos.

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <mpi.h>
#include <ctype.h> // Necessário para a função isspace, usada para limpar strings

// =====================================================================================
// --- ETAPA 0: CONFIGURAÇÃO GLOBAL ---
// Explicação: Aqui definimos as constantes que controlam o comportamento do programa.
// Usar constantes torna o código mais fácil de ler e modificar no futuro.
// =====================================================================================

// Explicação: Caminhos absolutos para os arquivos de dados. Isso evita erros de "arquivo não encontrado",
// não importando de qual diretório o programa seja executado.
const char* ARQUIVO_MAPA_CURSOS = "/home/luiz/Downloads/MPI-ENADE-main/2.DADOS/microdados2021_arq1.txt";
const char* NOMES_ARQUIVOS_PERGUNTAS[] = {
    "/home/luiz/Downloads/MPI-ENADE-main/2.DADOS/microdados2021_arq5.txt",   // Contém a variável TP_SEXO
    "/home/luiz/Downloads/MPI-ENADE-main/2.DADOS/microdados2021_arq21.txt",  // QE_I15 (Ação Afirmativa)
    "/home/luiz/Downloads/MPI-ENADE-main/2.DADOS/microdados2021_arq24.txt",  // QE_I18 (Modalidade Ensino Médio)
    "/home/luiz/Downloads/MPI-ENADE-main/2.DADOS/microdados2021_arq25.txt",  // QE_I19 (Incentivo)
    "/home/luiz/Downloads/MPI-ENADE-main/2.DADOS/microdados2021_arq27.txt",  // QE_I21 (Família com Superior)
    "/home/luiz/Downloads/MPI-ENADE-main/2.DADOS/microdados2021_arq28.txt",  // QE_I22 (Livros)
    "/home/luiz/Downloads/MPI-ENADE-main/2.DADOS/microdados2021_arq29.txt"   // QE_I23 (Horas de Estudo)
};
const int NUM_ARQUIVOS_PERGUNTAS = 7;
const long CODIGO_GRUPO_ADS_NUM = 72; // O código oficial para a área de Análise e Desenvolvimento de Sistemas

#define MAX_LINE_LEN 256 // Define um buffer de segurança para ler linhas dos arquivos.
#define NUM_RESULTS 15   // O número total de contadores que precisamos para todas as nossas perguntas.

/* MAPEAMENTO DOS CONTADORES DE RESULTADOS (ÍNDICES DO ARRAY):
 * 0: [Q1] Total de alunos em Ação Afirmativa
 * 1: [Q2] Total de MULHERES em Ação Afirmativa
 * 2: [Q3] Total de Mulheres em A.A. que fizeram Ensino Técnico (QE_I18, Resposta 'B')
 * 3: [Q4] Total de Mulheres em A.A. com incentivo dos PAIS (QE_I19, Resposta 'B')
 * 4: [Q5] Total de alunos em A.A. com família com curso superior (QE_I21, Resposta 'A')
 * -- [Q6] Distribuição de Livros Lidos --
 * 5: 'A', 6: 'B', 7: 'C', 8: 'D', 9: 'E'
 * -- [Q7] Distribuição de Horas de Estudo --
 * 10: 'A', 11: 'B', 12: 'C', 13: 'D', 14: 'E'
 */

// --- FUNÇÕES AUXILIARES ---

// Explicação: Esta função "limpa" uma string, removendo aspas e espaços em branco (no início, meio ou fim).
// É uma etapa CRÍTICA para garantir que as comparações de string (ex: strcmp(sexo, "F")) funcionem de forma confiável.
void trim_quotes_and_spaces(char *str) {
    if (str == NULL) return;
    int i, j = 0;
    for (i = 0; str[i]; i++) {
        if (str[i] != '"' && !isspace((unsigned char)str[i])) {
            str[j++] = str[i];
        }
    }
    str[j] = '\0';
}

// Explicação: Esta função verifica se um código de curso (`co_curso`) está na nossa lista de cursos de ADS.
// Ela é usada para filtrar apenas os dados que nos interessam.
int is_ads_course(const char* co_curso, char** ads_course_list, int count) {
    for (int i = 0; i < count; i++) { if (strcmp(co_curso, ads_course_list[i]) == 0) return 1; }
    return 0;
}

// Explicação: Esta é a função de análise principal. Ela é executada em PARALELO por TODOS os processos.
// Cada processo recebe um "bloco" de texto com dados já unificados e incrementa seus contadores locais.
void analisar_bloco(char* bloco, long* resultados_locais) {
    char* linha_saveptr;
    char* linha = strtok_r(bloco, "\n", &linha_saveptr);
    while (linha != NULL) {
        char* campos[10]; int i = 0;
        char* campo_saveptr;
        char* token = strtok_r(linha, ";", &campo_saveptr);
        while (token != NULL && i < 10) { campos[i++] = token; token = strtok_r(NULL, ";", &campo_saveptr); }
        if (i >= 9) { // Garante que a linha tem todos os campos esperados
            // Explicação: Limpa todos os campos de resposta antes de usá-los.
            for(int k=2; k<9; k++) trim_quotes_and_spaces(campos[k]);
            
            const char* sexo = campos[2], *acao_afirmativa = campos[3], *modalidade_ens_medio = campos[4], *incentivo = campos[5], *familia_superior = campos[6], *livros = campos[7], *horas_estudo = campos[8];
            
            // Explicação: Realiza as comparações lógicas com os dados já limpos.
            int ehAcaoAfirmativa = (strcmp(acao_afirmativa, "A") != 0); // Resposta 'A' é "Não".
            int ehFeminino = (strcmp(sexo, "F") == 0);

            // Bloco de Perguntas com filtro de Ação Afirmativa
            if (ehAcaoAfirmativa) {
                resultados_locais[0]++; // [Q1]
                if (strcmp(familia_superior, "A") == 0) resultados_locais[4]++; // [Q5]
                if (ehFeminino) {
                    resultados_locais[1]++; // [Q2]
                    if (strcmp(modalidade_ens_medio, "B") == 0) resultados_locais[2]++; // [Q3]
                    if (strcmp(incentivo, "B") == 0) resultados_locais[3]++; // [Q4]
                }
            }
            // Bloco de Pergunta 6 (Livros Lidos) - INDEPENDENTE
            if (strcmp(livros, "A") == 0) resultados_locais[5]++; else if (strcmp(livros, "B") == 0) resultados_locais[6]++; else if (strcmp(livros, "C") == 0) resultados_locais[7]++; else if (strcmp(livros, "D") == 0) resultados_locais[8]++; else if (strcmp(livros, "E") == 0) resultados_locais[9]++;
            // Bloco de Pergunta 7 (Horas de Estudo) - INDEPENDENTE
            if (strcmp(horas_estudo, "A") == 0) resultados_locais[10]++; else if (strcmp(horas_estudo, "B") == 0) resultados_locais[11]++; else if (strcmp(horas_estudo, "C") == 0) resultados_locais[12]++; else if (strcmp(horas_estudo, "D") == 0) resultados_locais[13]++; else if (strcmp(horas_estudo, "E") == 0) resultados_locais[14]++;
        }
        linha = strtok_r(NULL, "\n", &linha_saveptr);
    }
}

int main(int argc, char** argv) {
    // --- ETAPA 1: INICIALIZAÇÃO DO MPI ---
    MPI_Init(&argc, &argv);
    int rank, n_procs;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank); // Cada processo descobre sua ID (0 a 3)
    MPI_Comm_size(MPI_COMM_WORLD, &n_procs); // Todos descobrem que há 4 processos
    long resultados_locais[NUM_RESULTS] = {0}; // Cada processo zera sua "prancheta" de contagem.

    // --- ETAPA 2: O GERENTE (RANK 0) FAZ TODO O TRABALHO DE PREPARAÇÃO ---
    if (rank == 0) {
        // --- ETAPA 2.1: Mapear os Cursos de ADS ---
        // Explicação: Lê o arq1.txt para criar uma lista de todos os CO_CURSO que pertencem ao CO_GRUPO de ADS (72).
        printf("Mestre (Rank 0): Mapeando cursos de ADS...\n");
        FILE* fp_mapa = fopen(ARQUIVO_MAPA_CURSOS, "r");
        if (!fp_mapa) { fprintf(stderr, "Erro fatal: Falha ao abrir mapa de cursos. Verifique se o caminho do arquivo está correto.\n"); MPI_Abort(MPI_COMM_WORLD, 1); }
        char** ads_courses_list = NULL;
        int ads_courses_count = 0;
        char line[MAX_LINE_LEN];
        fgets(line, sizeof(line), fp_mapa);
        while (fgets(line, sizeof(line), fp_mapa)) {
            char* line_copy = strdup(line);
            char* co_curso_str, *co_grupo_str;
            strtok(line_copy, ";"); co_curso_str = strtok(NULL, ";"); strtok(NULL, ";"); strtok(NULL, ";"); strtok(NULL, ";"); co_grupo_str = strtok(NULL, ";");
            if (co_grupo_str && co_curso_str) {
                if (atol(co_grupo_str) == CODIGO_GRUPO_ADS_NUM) { // Comparação numérica robusta
                    trim_quotes_and_spaces(co_curso_str);
                    ads_courses_count++;
                    ads_courses_list = realloc(ads_courses_list, ads_courses_count * sizeof(char*));
                    ads_courses_list[ads_courses_count - 1] = strdup(co_curso_str);
                }
            }
            free(line_copy);
        }
        fclose(fp_mapa);
        printf("Mestre (Rank 0): Mapeamento concluído. %d cursos de ADS encontrados. Unificando dados...\n", ads_courses_count);
        
        // --- ETAPA 2.2: Unificar os Dados das Perguntas ---
        // Explicação: Lê os 7 arquivos de perguntas linha por linha. Se a linha pertence a um curso de ADS (verificado na lista),
        // combina as respostas em uma "super-linha" e armazena em um grande buffer na memória.
        FILE* readers[NUM_ARQUIVOS_PERGUNTAS];
        for (int i = 0; i < NUM_ARQUIVOS_PERGUNTAS; i++) {
            readers[i] = fopen(NOMES_ARQUIVOS_PERGuntas[i], "r");
            if (!readers[i]) { fprintf(stderr, "Erro fatal: Falha ao abrir o arquivo %s\n", NOMES_ARQUIVOS_PERGuntas[i]); MPI_Abort(MPI_COMM_WORLD, 1); }
            fgets(line, sizeof(line), readers[i]);
        }
        size_t buffer_size = 1024 * 1024;
        char* dados_unificados = malloc(buffer_size);
        size_t current_pos = 0;
        dados_unificados[0] = '\0';
        char linhas[NUM_ARQUIVOS_PERGUNTAS][MAX_LINE_LEN];
        while(fgets(linhas[0], sizeof(linhas[0]), readers[0]) != NULL) {
            int linha_valida = 1;
            for(int i = 1; i < NUM_ARQUIVOS_PERGUNTAS; i++) { if (fgets(linhas[i], sizeof(linhas[i]), readers[i]) == NULL) { linha_valida = 0; break; } }
            if (!linha_valida) break;
            char* curso_copy = strdup(linhas[0]);
            char* ano = strtok(curso_copy, ";");
            char* curso = strtok(NULL, ";");
            trim_quotes_and_spaces(curso);
            if (curso && is_ads_course(curso, ads_courses_list, ads_courses_count)) {
                char linha_combinada[MAX_LINE_LEN * 2];
                trim_quotes_and_spaces(ano);
                sprintf(linha_combinada, "%s;%s", ano, curso);
                for(int i = 0; i < NUM_ARQUIVOS_PERGuntas; i++) {
                    char* resp_copy = strdup(linhas[i]);
                    strtok(resp_copy, ";"); strtok(NULL, ";"); 
                    char* resposta = strtok(NULL, "\n\r");
                    strcat(linha_combinada, ";");
                    if (resposta) strcat(linha_combinada, resposta);
                    free(resp_copy);
                }
                strcat(linha_combinada, "\n");
                if (current_pos + strlen(linha_combinada) + 1 > buffer_size) {
                    buffer_size *= 2;
                    dados_unificados = realloc(dados_unificados, buffer_size);
                }
                strcpy(dados_unificados + current_pos, linha_combinada);
                current_pos += strlen(linha_combinada);
            }
            free(curso_copy);
        }
        for (int i = 0; i < NUM_ARQUIVOS_PERGuntas; i++) fclose(readers[i]);
        for (int i = 0; i < ads_courses_count; i++) free(ads_courses_list[i]);
        free(ads_courses_list);
        
        // --- ETAPA 2.3: Distribuir o Trabalho ---
        // Explicação: O Gerente divide o buffer de dados unificados em partes iguais e envia um pedaço para cada trabalhador.
        printf("Mestre (Rank 0): Leitura concluída. Total de %zu bytes para TADS. Distribuindo trabalho...\n", current_pos);
        long chunk_size = current_pos > 0 ? current_pos / n_procs : 0;
        for (int i = 1; i < n_procs; i++) {
            long start = i * chunk_size;
            long size_to_send = (i == n_procs - 1) ? (current_pos - start) : chunk_size;
            MPI_Send(&size_to_send, 1, MPI_LONG, i, 0, MPI_COMM_WORLD);
            if (size_to_send > 0) MPI_Send(dados_unificados + start, size_to_send, MPI_CHAR, i, 0, MPI_COMM_WORLD);
        }
        // Explicação: O Gerente também analisa sua própria parte dos dados.
        if(chunk_size > 0) {
            char* my_bloco = malloc(chunk_size + 1);
            strncpy(my_bloco, dados_unificados, chunk_size);
            my_bloco[my_chunk_size] = '\0';
            analisar_bloco(my_bloco, resultados_locais);
            free(my_bloco);
        }
        free(dados_unificados);
    } 
    // --- ETAPA 3: TRABALHADORES (RANK 1, 2, 3) RECEBEM E PROCESSAM ---
    else {
        long size_to_recv = 0;
        MPI_Recv(&size_to_recv, 1, MPI_LONG, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        if (size_to_recv > 0) {
            char* bloco_recebido = malloc(size_to_recv + 1);
            MPI_Recv(bloco_recebido, size_to_recv, MPI_CHAR, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            bloco_recebido[size_to_recv] = '\0';
            analisar_bloco(bloco_recebido, resultados_locais);
            free(bloco_recebido);
        }
    }

    // --- ETAPA 4: AGREGAÇÃO DOS RESULTADOS ---
    // Explicação: Todos os processos participam. O MPI soma os valores de todas as pranchetas `resultados_locais`
    // e armazena o resultado final na prancheta `resultados_globais` do Gerente (rank 0).
    long resultados_globais[NUM_RESULTS] = {0};
    MPI_Reduce(resultados_locais, resultados_globais, NUM_RESULTS, MPI_LONG, MPI_SUM, 0, MPI_COMM_WORLD);

    // --- ETAPA 5: IMPRESSÃO DO RELATÓRIO FINAL ---
    // Explicação: Apenas o Gerente, que tem os dados consolidados, imprime o resultado.
    if (rank == 0) {
        long totalAfirmativa = resultados_globais[0], mulheresAfirmativa = resultados_globais[1], mulheresTecnico = resultados_globais[2];
        printf("\n--- RESULTADOS DA ANÁLISE ENADE 2021 (TADS) ---\n\n");
        printf("1. Total de alunos que ingressaram por Ações Afirmativas: %ld\n", totalAfirmativa);
        printf("2. Porcentagem de mulheres dentre os que ingressaram por Ações Afirmativas: %.2f%%\n", totalAfirmativa > 0 ? (double)mulheresAfirmativa / totalAfirmativa * 100.0 : 0);
        printf("3. Dentre as mulheres de A.A., porcentagem que concluiu Ensino Técnico: %.2f%%\n", mulheresAfirmativa > 0 ? (double)mulheresTecnico / mulheresAfirmativa * 100.0 : 0);
        printf("4. Dentre as mulheres de A.A., total que recebeu maior incentivo dos Pais: %ld\n", resultados_globais[3]);
        printf("5. Dentre os alunos de A.A., total com familiares com curso superior: %ld\n", resultados_globais[4]);
        printf("\n6. Quantos livros os alunos de TADS leram no ano?\n");
        printf("   - Nenhum (A):.................. %ld\n", resultados_globais[5]);
        printf("   - Um ou dois (B):.............. %ld\n", resultados_globais[6]);
        printf("   - Três a cinco (C):............ %ld\n", resultados_globais[7]);
        printf("   - Seis a oito (D):............. %ld\n", resultados_globais[8]);
        printf("   - Mais de oito (E):............ %ld\n", resultados_globais[9]);
        printf("\n7. Quantas horas semanais os alunos de TADS se dedicaram aos estudos?\n");
        printf("   - Nenhuma (A):................. %ld\n", resultados_globais[10]);
        printf("   - De uma a três (B):........... %ld\n", resultados_globais[11]);
        printf("   - De quatro a sete (C):........ %ld\n", resultados_globais[12]);
        printf("   - De oito a doze (D):.......... %ld\n", resultados_globais[13]);
        printf("   - Mais de doze (E):............ %ld\n", resultados_globais[14]);
    }

    // --- ETAPA 6: FINALIZAÇÃO ---
    MPI_Finalize();
    return 0;
}
